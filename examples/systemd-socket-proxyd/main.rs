// SPDX-License-Identifier: LGPL-2.1-or-later

//! `systemd-socket-proxyd`: bidirectionally proxy local sockets to another
//! (possibly remote) socket.
//!
//! A drop-in reimplementation of systemd's `systemd-socket-proxyd(8)`.
//! `tests/example_systemd_socket_proxyd.rs` runs every scenario against
//! this and against the original, and requires the same observable
//! behaviour; the few intended differences are listed there.
//!
//! For each connection on the sockets it is passed (socket activation),
//! it resolves the remote host, connects to it, optionally sends a PROXY
//! protocol v1 header, and then forwards data in both directions with
//! `splice(2)` until both have ended.
//!
//! ```sh
//! cargo build --example systemd-socket-proxyd
//! systemd-socket-activate -l 8080 target/debug/examples/systemd-socket-proxyd localhost:7777
//! ```
//!
//! forwards connections on port 8080 to the greeter example on port 7777.

#![forbid(unsafe_code)]

mod cli;
mod introspect;

use std::future::pending;
use std::io::{self, ErrorKind, Write};
use std::net::SocketAddr;
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::process::ExitCode;
use std::rc::Rc;
use std::time::Duration;

use cli::{Action, Config, Remote};
use sd_event_futures::combinator::race_all;
use sd_event_futures::daemon;
use sd_event_futures::executor::{block_on, spawn, spawn_blocking};
use sd_event_futures::forward::{forward, is_disconnect};
use sd_event_futures::reactor::{AsyncFd, connect_tcp, connect_unix, signal, sleep};
use sd_event_futures::sync::{Gate, TaskTracker};
use sd_event_futures::{Event, log, os, select};

const PROXY_HEADER_TIMEOUT: Duration = Duration::from_secs(10);

pub fn main() -> ExitCode {
    let action = match cli::parse_args(std::env::args().skip(1)) {
        Ok(action) => action,
        Err(message) => {
            log!(Error, "{message}");
            return ExitCode::FAILURE;
        }
    };
    let output = match action {
        Action::Help => cli::help(),
        Action::Version => cli::version(),
        Action::IntrospectCli => introspect::introspect() + "\n",
        Action::Run(config) => {
            return match block_on(run(config)) {
                Ok(()) => ExitCode::SUCCESS,
                Err(e) => {
                    log!(Error, "{e}");
                    ExitCode::FAILURE
                }
            };
        }
    };
    match io::stdout().write_all(output.as_bytes()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(_) => ExitCode::FAILURE,
    }
}

async fn run(config: Config) -> io::Result<()> {
    // Each connection holds several fds (two sockets and a pipe per
    // direction), so the default soft limit of 1024 runs out well before
    // the default connection limit.
    os::raise_open_files_limit();
    Event::thread_default()?.set_watchdog(true)?;

    let listeners = listeners()?;
    let mut sigterm = signal(libc::SIGTERM)?;
    let mut sigint = signal(libc::SIGINT)?;
    daemon::notify("READY=1\nSTATUS=Processing requests...")?;

    let config = Rc::new(config);
    let connections = TaskTracker::new();
    // Started by socket activation, a connection is always on its way: the
    // idle timer only starts once one has come, or an accept failed.
    let idle_timer_armed = Gate::closed();
    loop {
        select! {
            _ = sigterm.recv() => break,
            _ = sigint.recv() => break,
            () = idle_exit(&connections, &idle_timer_armed, config.exit_idle_time) => break,
            (_, accepted) = race_all(listeners.iter().map(accept)) => {
                idle_timer_armed.open();
                match accepted {
                    Ok(_refused) if connections.len() >= config.connections_max as usize => {
                        log!(Warning, "Hit connection limit, refusing connection.");
                    }
                    Ok(client) => {
                        spawn(connections.track(connection(config.clone(), client)));
                    }
                    Err(e) if is_accept_again(&e) => {}
                    Err(e) => log!(Warning, "Failed to accept() socket, ignoring: {}", os::errno_message(&e)),
                }
            },
        }
    }
    daemon::notify("STOPPING=1\nSTATUS=Shutting down...")?;
    Ok(())
}

/// The sockets systemd passed in, each of which must be a listening
/// stream socket.
fn listeners() -> io::Result<Vec<AsyncFd<OwnedFd>>> {
    let sockets = daemon::listen_fds();
    if sockets.is_empty() {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "Didn't get any sockets passed in.",
        ));
    }
    sockets
        .into_iter()
        .map(|socket| {
            let stream = os::is_listening_stream(socket.fd.as_fd())
                .map_err(failed("Failed to determine socket type"))?;
            if !stream {
                return Err(io::Error::new(
                    ErrorKind::InvalidInput,
                    "Passed in socket is not a stream socket.",
                ));
            }
            AsyncFd::new(socket.fd)
        })
        .collect()
}

async fn accept(listener: &AsyncFd<OwnedFd>) -> io::Result<OwnedFd> {
    listener
        .read_with(|socket| os::accept(socket.as_fd()))
        .await
}

/// `ERRNO_IS_ACCEPT_AGAIN()`: the connection went away before it was
/// accepted, or the like; just carry on.
fn is_accept_again(e: &io::Error) -> bool {
    is_disconnect(e)
        || matches!(
            e.raw_os_error(),
            Some(libc::EAGAIN | libc::EINTR | libc::EOPNOTSUPP)
        )
}

/// Resolves once the armed idle timer has seen no connection for
/// `idle_time`. Recreated on every turn of the accept loop, which restarts
/// the timer.
async fn idle_exit(connections: &TaskTracker, armed: &Gate, idle_time: Option<Duration>) {
    let Some(idle_time) = idle_time else {
        return pending().await;
    };
    armed.wait_open().await;
    loop {
        connections.idle().wait_open().await;
        select! {
            () = sleep(idle_time) => return,
            () = connections.idle().wait_closed() => {},
        }
    }
}

async fn connection(config: Rc<Config>, client: OwnedFd) {
    if let Err(e) = proxy(&config, client).await {
        log!(Error, "{e}");
    }
}

/// Reach the remote end for `client`, then relay between the two.
async fn proxy(config: &Config, client: OwnedFd) -> io::Result<()> {
    let client = AsyncFd::new(client)?;
    match &config.remote {
        Remote::Unix(address) => {
            let server =
                connect_unix(address).map_err(failed("Failed to connect to remote host"))?;
            relay(config, &client, &server).await
        }
        Remote::Inet { node, service } => {
            let address = resolve(node, service)
                .await
                .map_err(failed("Failed to resolve host"))?;
            let server = connect_tcp(address)
                .await
                .map_err(failed("Failed to connect to remote host"))?;
            relay(config, &client, &server).await
        }
    }
}

/// `getaddrinfo()` blocks, so it runs on the blocking pool.
async fn resolve(node: &str, service: &str) -> io::Result<SocketAddr> {
    let (node, service) = (node.to_owned(), service.to_owned());
    spawn_blocking(move || os::resolve(&node, &service)).await
}

async fn relay<S: AsFd>(
    config: &Config,
    client: &AsyncFd<OwnedFd>,
    server: &AsyncFd<S>,
) -> io::Result<()> {
    if config.proxy_protocol {
        let header = proxy_header(client.get_ref().as_fd());
        select! {
            sent = server.send_all(header.as_bytes()) => sent.map_err(failed("Failed to write to backend host"))?,
            () = sleep(PROXY_HEADER_TIMEOUT) => {
                return Err(failed("Failed to write to backend host")(io::Error::from_raw_os_error(libc::ETIMEDOUT)));
            },
        }
    }
    forward(client, server)
        .await
        .map_err(failed("Forwarding failed"))
}

/// The PROXY protocol v1 header describing `client`'s connection, or
/// `UNKNOWN` for anything but TCP over IPv4 or IPv6.
fn proxy_header(client: BorrowedFd<'_>) -> String {
    let header = |protocol, peer: SocketAddr, local: SocketAddr| {
        format!(
            "PROXY {protocol} {} {} {} {}\r\n",
            peer.ip(),
            local.ip(),
            peer.port(),
            local.port()
        )
    };
    match (os::peer_address(client), os::local_address(client)) {
        (Some(peer @ SocketAddr::V4(_)), Some(local @ SocketAddr::V4(_))) => {
            header("TCP4", peer, local)
        }
        (Some(peer @ SocketAddr::V6(_)), Some(local @ SocketAddr::V6(_))) => {
            header("TCP6", peer, local)
        }
        _ => {
            log!(
                Warning,
                "Only TCP over IPv4 and IPv6 are supported, reporting fallback proxy data 'UNKNOWN'."
            );
            "PROXY UNKNOWN\r\n".to_owned()
        }
    }
}

/// Like systemd's `log_error_errno(r, "WHAT: %m")`: `what`, then the
/// error's `strerror()` text.
fn failed(what: &'static str) -> impl Fn(io::Error) -> io::Error {
    move |e| io::Error::new(e.kind(), format!("{what}: {}", os::errno_message(&e)))
}
