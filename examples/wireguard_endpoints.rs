// SPDX-License-Identifier: LGPL-2.1-or-later

//! networkd: resolving WireGuard peer endpoints, retrying with backoff.
//!
//! A WireGuard peer can name its endpoint by hostname, which often doesn't
//! resolve yet at boot. networkd resolves each peer's endpoint and, on
//! failure, retries after an exponentially growing, jittered delay (200ms
//! doubling up to 25.6s). Per peer it tracks a retry counter, a retry
//! timer and a pending query, and guards against starting a lookup while
//! either of the latter is active.
//!
//! - https://github.com/systemd/systemd/blob/f1d45e3d1466b24ceed38e3a54289ecf31edf9d1/src/network/netdev/wireguard.c#L305-L446
//!
//! Here each peer is a future with the retry counter as a local variable,
//! and the peers are resolved concurrently with `join_all`. The C code also
//! stops starting lookups when sd-resolve's query table is full (ENOBUFS)
//! and resumes later; the blocking pool's queue makes that unnecessary.
//!
//! Verdict: much more idiomatic. Three fields and two "already in
//! progress?" checks per peer turn into `loop { lookup; sleep }`.
//!
//! ```sh
//! cargo run --example wireguard_endpoints [HOST:PORT...]
//! ```
//!
//! resolves the endpoints, retrying those that fail. Add a missing name to
//! `/etc/hosts` while it retries, and watch it get picked up.
//!
//! Links to the C code are pinned to systemd commit f1d45e3d.

#![forbid(unsafe_code)]

use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::time::Duration;

use sd_event_futures::combinator::join_all;
use sd_event_futures::executor::{block_on, spawn_blocking};
use sd_event_futures::reactor::sleep;
use sd_event_futures::{log, os, select};

const EXAMPLES: [&str; 2] = ["localhost:51820", "peer.wireguard.invalid:51820"];
/// The base of the backoff: the first retry comes after twice this.
const UNIT: Duration = Duration::from_millis(100);
/// How long the demonstration runs; networkd never gives up.
const DEMONSTRATION: Duration = Duration::from_secs(30);

/// Resolves an endpoint, blocking; `getaddrinfo()` in networkd.
pub type Resolver = fn(&str) -> io::Result<Vec<SocketAddr>>;

pub fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let endpoints: Vec<&str> = if args.is_empty() {
        EXAMPLES.to_vec()
    } else {
        args.iter().map(String::as_str).collect()
    };
    block_on(async {
        select! {
            _ = resolve_peer_endpoints(&endpoints, getaddrinfo, UNIT) => {},
            () = sleep(DEMONSTRATION) => println!("Stopping after {DEMONSTRATION:?}."),
        }
    });
}

fn getaddrinfo(endpoint: &str) -> io::Result<Vec<SocketAddr>> {
    Ok(endpoint.to_socket_addrs()?.collect())
}

pub async fn resolve_peer_endpoints(
    endpoints: &[&str],
    resolve: Resolver,
    unit: Duration,
) -> Vec<SocketAddr> {
    join_all(endpoints.iter().map(|e| resolve_endpoint(e, resolve, unit))).await
}

/// The first address of `endpoint`, once it resolves.
pub async fn resolve_endpoint(endpoint: &str, resolve: Resolver, unit: Duration) -> SocketAddr {
    let mut retries = 0;
    loop {
        let name = endpoint.to_owned();
        let failure = match spawn_blocking(move || resolve(&name)).await {
            Ok(addresses) => match addresses.first() {
                Some(&address) => {
                    log!(Info, "Resolved {endpoint} to {address}.");
                    return address;
                }
                None => format!("Neither IPv4 nor IPv6 address found for {endpoint}"),
            },
            Err(e) => format!("Failed to resolve host '{endpoint}': {e}"),
        };
        retries += 1;
        let delay = next_resolve_delay(retries, unit);
        log!(Warning, "{failure}, retrying in {delay:.1?}.");
        sleep(delay).await;
    }
}

/// `peer_next_resolve_usec()`: twice `unit` (100ms in networkd) times
/// 2^min(retries, 7), minus up to 10% jitter.
pub fn next_resolve_delay(retries: u32, unit: Duration) -> Duration {
    let delay = unit * (2u32 << retries.min(7));
    let jitter = delay.as_nanos() as u64 / 10;
    delay - Duration::from_nanos(jitter) + Duration::from_nanos(os::random_u64() % jitter.max(1))
}
