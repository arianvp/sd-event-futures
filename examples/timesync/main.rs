// SPDX-License-Identifier: LGPL-2.1-or-later

//! timesyncd: find a time server that answers, and read its clock.
//!
//! timesyncd walks its configured servers. For each name it resolves the
//! addresses, and for each address it sends an NTP request and arms a
//! timeout; a resolve failure, a bad reply and a timeout all go back to
//! `manager_connect()`, which works out from `current_server_name` and
//! `current_server_address` what the next candidate is.
//!
//! - manager_connect(): https://github.com/systemd/systemd/blob/f1d45e3d1466b24ceed38e3a54289ecf31edf9d1/src/timesync/timesyncd-manager.c#L907-L1023
//! - resolve callback: https://github.com/systemd/systemd/blob/f1d45e3d1466b24ceed38e3a54289ecf31edf9d1/src/timesync/timesyncd-manager.c#L740-L786
//! - request and timeout: https://github.com/systemd/systemd/blob/f1d45e3d1466b24ceed38e3a54289ecf31edf9d1/src/timesync/timesyncd-manager.c#L88-L177
//!
//! Here "next candidate" is two nested `for` loops, and a timeout is a
//! `select!` with a timer. Only the search and one measurement are
//! modelled: the poll interval, spike detection and the clock adjustment
//! are left out (`ntp.rs` holds the packet format).
//!
//! Verdict: much more idiomatic. The C code is correct but hard to follow:
//! the iteration order lives in `manager_connect()`'s if/else chain and in
//! three callbacks that each re-enter it.
//!
//! ```sh
//! cargo run --example timesync [SERVER...]
//! ```
//!
//! asks the given servers, or systemd's default ones, how far this
//! machine's clock is off.
//!
//! Links to the C code are pinned to systemd commit f1d45e3d.

#![forbid(unsafe_code)]

pub mod ntp;

use std::io::{self, ErrorKind};
use std::net::{SocketAddr, ToSocketAddrs, UdpSocket};
use std::process::ExitCode;
use std::time::{Duration, SystemTime};

use sd_event_futures::executor::{block_on, spawn_blocking};
use sd_event_futures::reactor::{AsyncFd, sleep};
use sd_event_futures::{log, os, select};

pub use ntp::Sample;

/// systemd's default `ntp-servers` build option.
const DEFAULT_SERVERS: [&str; 4] = [
    "time1.google.com",
    "time2.google.com",
    "time3.google.com",
    "time4.google.com",
];
/// timesyncd's `TIMEOUT_USEC`.
const REPLY_TIMEOUT: Duration = Duration::from_secs(10);

pub fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let names: Vec<&str> = if args.is_empty() {
        DEFAULT_SERVERS.to_vec()
    } else {
        args.iter().map(String::as_str).collect()
    };
    let servers: Vec<_> = names.into_iter().map(|name| (name, ntp::PORT)).collect();
    match block_on(find_time_server(&servers, REPLY_TIMEOUT)) {
        Ok(server) => {
            println!("{} ({}): {}", server.name, server.address, server.sample);
            ExitCode::SUCCESS
        }
        Err(e) => {
            log!(Error, "{e}");
            ExitCode::FAILURE
        }
    }
}

/// The first server that answered, and what it said.
#[derive(Debug)]
pub struct Server {
    pub name: String,
    pub address: SocketAddr,
    pub sample: Sample,
}

/// Try each address of each of `servers` in turn, until one answers
/// within `reply_timeout`.
pub async fn find_time_server(
    servers: &[(&str, u16)],
    reply_timeout: Duration,
) -> io::Result<Server> {
    for &(name, port) in servers {
        let addresses = match lookup(name, port).await {
            Ok(addresses) => addresses,
            Err(e) => {
                log!(Warning, "Failed to resolve {name}: {e}");
                continue;
            }
        };
        for address in addresses {
            match query(address, reply_timeout).await {
                Ok(sample) => {
                    return Ok(Server {
                        name: name.to_owned(),
                        address,
                        sample,
                    });
                }
                Err(e) => log!(Warning, "{name} ({address}): {e}"),
            }
        }
    }
    Err(io::Error::new(ErrorKind::NotFound, "No server found."))
}

async fn lookup(name: &str, port: u16) -> io::Result<Vec<SocketAddr>> {
    let name = name.to_owned();
    spawn_blocking(move || Ok((name.as_str(), port).to_socket_addrs()?.collect())).await
}

/// Ask the server at `address` for the time.
async fn query(address: SocketAddr, reply_timeout: Duration) -> io::Result<Sample> {
    let local = if address.is_ipv4() {
        "0.0.0.0:0"
    } else {
        "[::]:0"
    };
    let socket = UdpSocket::bind(local)?;
    // Connected, the socket only receives from the server, and an ICMP
    // error comes back as a failed receive.
    socket.connect(address)?;
    let socket = AsyncFd::new(socket)?;

    let nonce = os::random_u64();
    let sent = SystemTime::now();
    socket.write_with(|s| s.send(&ntp::request(nonce))).await?;
    let mut reply = [0; ntp::PACKET_SIZE];
    select! {
        received = socket.read_with(|s| s.recv(&mut reply)) => match received? {
            ntp::PACKET_SIZE => ntp::sample(&reply, nonce, sent, SystemTime::now()),
            _ => Err(io::Error::new(ErrorKind::InvalidData, "Invalid packet size.")),
        },
        () = sleep(reply_timeout) => Err(io::Error::new(ErrorKind::TimedOut, "Timed out waiting for reply.")),
    }
}
