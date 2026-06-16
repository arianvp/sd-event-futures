// SPDX-License-Identifier: LGPL-2.1-or-later

//! sd-resolve: `getaddrinfo(3)` without blocking the loop.
//!
//! `getaddrinfo()` blocks, potentially for seconds, and has no
//! non-blocking variant. sd-resolve runs it on a pool that grows on demand
//! to at most 16 worker threads, fed through two socketpairs. Since only
//! bytes cross a socket, every request and every `struct addrinfo` answer
//! is serialized and parsed again by hand, and answers are matched to
//! queries by id. Callers get the result in a
//! `sd_resolve_getaddrinfo_handler_t` callback.
//!
//! - worker threads:
//!   https://github.com/systemd/systemd/blob/f1d45e3d1466b24ceed38e3a54289ecf31edf9d1/src/libsystemd/sd-resolve/sd-resolve.c#L404-L470
//! - socketpairs: https://github.com/systemd/systemd/blob/f1d45e3d1466b24ceed38e3a54289ecf31edf9d1/src/libsystemd/sd-resolve/sd-resolve.c#L497-L511
//! - (un)serializing addrinfo:
//!   https://github.com/systemd/systemd/blob/f1d45e3d1466b24ceed38e3a54289ecf31edf9d1/src/libsystemd/sd-resolve/sd-resolve.c#L189-L280
//!   https://github.com/systemd/systemd/blob/f1d45e3d1466b24ceed38e3a54289ecf31edf9d1/src/libsystemd/sd-resolve/sd-resolve.c#L677-L828
//! - users: timesyncd (`examples/timesync`), networkd
//!   (`examples/wireguard_endpoints.rs`), socket-proxyd
//!   (`examples/systemd-socket-proxyd`).
//!
//! Here the pool is the crate's [`blocking`](sd_event_futures::blocking)
//! pool, capped at 16 threads the same way, and the answer is an ordinary
//! `Vec` moved between threads in memory: no wire format, no query ids,
//! and the callback is an `.await`.
//!
//! ```sh
//! cargo run --example resolve [NAME...]
//! ```
//!
//! resolves all names at once, so the whole run takes about as long as
//! the slowest lookup.
//!
//! Links to the C code are pinned to systemd commit f1d45e3d.

#![forbid(unsafe_code)]

use std::io;
use std::net::{IpAddr, ToSocketAddrs};
use std::time::Instant;

use sd_event_futures::combinator::join_all;
use sd_event_futures::executor::{block_on, spawn_blocking};

const EXAMPLES: [&str; 4] = [
    "localhost",
    "systemd.io",
    "kernel.org",
    "no-such-host.invalid",
];

pub fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let names: Vec<&str> = if args.is_empty() {
        EXAMPLES.to_vec()
    } else {
        args.iter().map(String::as_str).collect()
    };
    let started = Instant::now();
    let answers = block_on(join_all(names.iter().map(|name| async move {
        let started = Instant::now();
        (name, lookup(name).await, started.elapsed())
    })));
    for (name, answer, took) in answers {
        match answer {
            Ok(addresses) => println!("{name} ({took:.1?}): {addresses:?}"),
            Err(e) => println!("{name} ({took:.1?}): {e}"),
        }
    }
    println!(
        "All {} lookups took {:.1?}.",
        names.len(),
        started.elapsed()
    );
}

/// The addresses of `name`, looked up on the blocking pool.
pub async fn lookup(name: &str) -> io::Result<Vec<IpAddr>> {
    let name = name.to_owned();
    spawn_blocking(move || {
        let addresses = (name.as_str(), 0).to_socket_addrs()?;
        Ok(addresses.map(|a| a.ip()).collect())
    })
    .await
}
