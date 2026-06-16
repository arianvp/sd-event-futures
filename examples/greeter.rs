// SPDX-License-Identifier: LGPL-2.1-or-later

//! A greeting server set up the way a systemd service should be: every
//! client that connects receives a line of greeting, after which
//! everything it sends is echoed back.
//!
//! - **Socket activation**: listens on the socket systemd passes in.
//!   Started without one, it listens on 127.0.0.1 with a random port and
//!   puts that socket into the fd store, so a restart (or a crash) keeps
//!   the port and the connections queued on it.
//! - **`Type=notify-reload`**: reports `READY=1` once it serves, reloads
//!   its configuration on `SIGHUP` with `RELOADING=1` and
//!   `MONOTONIC_USEC=`, and keeps `STATUS=` current.
//! - **Watchdog**: the event loop pings the manager while it keeps
//!   running, so a hung loop gets the service restarted.
//! - **Memory pressure**: releases cached memory when the kernel (or
//!   `MemoryPressureWatch=`) reports pressure.
//! - **Shutdown**: `SIGTERM` and `SIGINT` send `STOPPING=1` and stop
//!   accepting. Open connections get five more seconds, announced with
//!   `EXTEND_TIMEOUT_USEC=`.
//! - **Configuration** comes from the `greeting` credential.
//! - **Failures** at start-up are reported with `ERRNO=`, and log lines
//!   carry journal priorities when stderr is connected to the journal.
//!
//! ```sh
//! cargo run --example greeter    # standalone, on a random port
//! systemd-socket-activate -l 7777 --fdname=greeting target/debug/examples/greeter
//! nc localhost 7777
//! ```
//!
//! As a unit:
//!
//! ```ini
//! # greeter.socket
//! [Socket]
//! ListenStream=7777
//! FileDescriptorName=greeting
//!
//! # greeter.service
//! [Service]
//! Type=notify-reload
//! ExecStart=/usr/local/bin/greeter
//! WatchdogSec=10s
//! MemoryPressureWatch=auto
//! FileDescriptorStoreMax=1
//! LoadCredential=greeting:/etc/greeter/greeting
//! ```

#![forbid(unsafe_code)]

use std::cell::RefCell;
use std::io::{self, ErrorKind, Read};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::fd::AsFd;
use std::path::Path;
use std::process::ExitCode;
use std::time::Duration;

use sd_event_futures::daemon;
use sd_event_futures::executor::{block_on, spawn};
use sd_event_futures::reactor::{AsyncFd, MemoryPressure, memory_pressure, signal, sleep};
use sd_event_futures::sync::TaskTracker;
use sd_event_futures::{Event, log, select, trim_memory};

const DEFAULT_GREETING: &str = "hello from sd-event futures";
const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

pub fn main() -> ExitCode {
    match block_on(run()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            log!(Error, "{e}");
            let _ = daemon::notify_failed(&e);
            ExitCode::FAILURE
        }
    }
}

async fn run() -> io::Result<()> {
    // From here on these signals are blocked and queued, so one that
    // arrives during start-up is handled once the loop runs.
    let mut sigterm = signal(libc::SIGTERM)?;
    let mut sigint = signal(libc::SIGINT)?;
    let mut sighup = signal(libc::SIGHUP)?;

    let watchdog = Event::thread_default()?.set_watchdog(true)?;

    let config = daemon::credential("greeting");
    let greeting = RefCell::new(load_greeting(config.as_deref())?);

    let listener = listener()?;
    let address = listener.local_addr()?;
    let listener = AsyncFd::new(listener)?;

    match memory_pressure() {
        Ok(pressure) => {
            spawn(release_memory_under(pressure));
        }
        Err(e) => log!(Debug, "Memory pressure notifications unavailable: {e}"),
    }

    log!(
        Info,
        "Listening on {address}, watchdog {}.",
        if watchdog { "enabled" } else { "disabled" }
    );
    daemon::notify_ready(&status(&greeting.borrow()))?;

    let connections = TaskTracker::new();
    // Earlier branches win ties, so a stream of connections can't hold off
    // a stop or a reload.
    loop {
        select! {
            _ = sigterm.recv() => break,
            _ = sigint.recv() => break,
            _ = sighup.recv() => reload(config.as_deref(), &greeting),
            accepted = listener.read_with(|l| l.accept()) => match accepted {
                Ok((stream, peer)) => {
                    spawn(connections.track(serve(stream, peer, greeting.borrow().clone())));
                }
                Err(e) => log!(Warning, "Failed to accept connection: {e}"),
            },
        }
    }

    daemon::notify_stopping("Shutting down.")?;
    drop(listener);
    if !connections.is_empty() {
        log!(
            Info,
            "Waiting up to {DRAIN_TIMEOUT:?} for {} connection(s).",
            connections.len()
        );
        daemon::notify_extend_timeout(DRAIN_TIMEOUT)?;
        select! {
            () = connections.idle().wait_open() => {},
            () = sleep(DRAIN_TIMEOUT) => log!(Warning, "Abandoning {} connection(s).", connections.len()),
        }
    }
    log!(Info, "Stopped.");
    Ok(())
}

/// The socket systemd passed in, or else a new one on 127.0.0.1 that goes
/// into the fd store, so that restarts keep it.
fn listener() -> io::Result<TcpListener> {
    if let Some(socket) = daemon::listen_fds().into_iter().next() {
        return Ok(TcpListener::from(socket.fd));
    }
    let listener = TcpListener::bind("127.0.0.1:0")?;
    // Without a store (or over vsock) restarts just get a new port.
    if let Err(e) = daemon::store_fd("listener", listener.as_fd()) {
        log!(
            Warning,
            "Failed to keep listening socket in the fd store: {e}"
        );
    }
    Ok(listener)
}

/// Release cached memory whenever the system runs short of it.
async fn release_memory_under(mut pressure: MemoryPressure) {
    loop {
        pressure.wait().await;
        let _ = trim_memory();
        log!(Notice, "Memory pressure, released cached memory.");
    }
}

/// `Type=notify-reload`: `RELOADING=1`, reload, then `READY=1` whether or
/// not the new configuration could be used.
fn reload(config: Option<&Path>, greeting: &RefCell<String>) {
    let _ = daemon::notify_reloading();
    // A small local file; reading it doesn't need to leave the loop.
    match load_greeting(config) {
        Ok(new) => {
            log!(Info, "Reloaded, greeting is now {new:?}.");
            *greeting.borrow_mut() = new;
        }
        Err(e) => log!(
            Warning,
            "Failed to reload configuration, keeping the old one: {e}"
        ),
    }
    let _ = daemon::notify_ready(&status(&greeting.borrow()));
}

fn load_greeting(path: Option<&Path>) -> io::Result<String> {
    let Some(path) = path else {
        return Ok(DEFAULT_GREETING.to_owned());
    };
    let greeting = std::fs::read_to_string(path)
        .map_err(|e| io::Error::new(e.kind(), format!("Failed to read {}: {e}", path.display())))?;
    let line = greeting.trim_end();
    if line.is_empty() || line.contains('\n') {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!("{} must contain exactly one non-empty line", path.display()),
        ));
    }
    Ok(line.to_owned())
}

fn status(greeting: &str) -> String {
    format!("Greeting with {greeting:?}")
}

async fn serve(stream: TcpStream, peer: SocketAddr, greeting: String) {
    if let Err(e) = greet_and_echo(stream, &greeting).await {
        log!(Warning, "Connection to {peer} failed: {e}");
    }
}

/// Send the greeting, then echo until the client hangs up.
async fn greet_and_echo(stream: TcpStream, greeting: &str) -> io::Result<()> {
    let stream = AsyncFd::new(stream)?;
    stream.send_all(format!("{greeting}\n").as_bytes()).await?;
    let mut buf = [0; 4096];
    loop {
        match stream.read_with(|mut s| s.read(&mut buf)).await? {
            0 => return Ok(()),
            n => stream.send_all(&buf[..n]).await?,
        }
    }
}
