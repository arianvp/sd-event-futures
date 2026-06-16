//! Tests of `examples/wireguard_endpoints.rs`.
#![deny(unsafe_code)]

mod common;
#[allow(dead_code)]
#[path = "../examples/wireguard_endpoints.rs"]
mod wireguard_endpoints;

use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use sd_event_futures::executor::block_on;

use common::within;
use wireguard_endpoints::{next_resolve_delay, resolve_peer_endpoints};

const LIMIT: Duration = Duration::from_secs(5);

static LATE_ATTEMPTS: AtomicU32 = AtomicU32::new(0);
static EARLY_ATTEMPTS: AtomicU32 = AtomicU32::new(0);

/// "late.example" only resolves on the third attempt, as if DNS came up
/// after networkd.
fn dns_coming_up(endpoint: &str) -> io::Result<Vec<SocketAddr>> {
    match endpoint {
        "late.example:51820" => match LATE_ATTEMPTS.fetch_add(1, Ordering::SeqCst) {
            0 => Err(io::Error::other("Temporary failure in name resolution")),
            1 => Ok(Vec::new()),
            _ => Ok(vec!["127.0.0.1:51820".parse().unwrap()]),
        },
        _ => {
            EARLY_ATTEMPTS.fetch_add(1, Ordering::SeqCst);
            Ok(vec!["127.0.0.1:51821".parse().unwrap()])
        }
    }
}

#[test]
fn retries_endpoints_until_they_resolve() {
    let unit = Duration::from_millis(1);
    let started = Instant::now();
    let endpoints = block_on(within(
        LIMIT,
        resolve_peer_endpoints(
            &["late.example:51820", "early.example:51821"],
            dns_coming_up,
            unit,
        ),
    ));
    assert_eq!(
        endpoints,
        [
            "127.0.0.1:51820".parse().unwrap(),
            "127.0.0.1:51821".parse().unwrap()
        ]
    );
    assert_eq!(LATE_ATTEMPTS.load(Ordering::SeqCst), 3);
    assert_eq!(EARLY_ATTEMPTS.load(Ordering::SeqCst), 1);
    // Two retries: at least 90% of 4 + 8 units.
    assert!(
        started.elapsed() >= unit * 108 / 10,
        "{:?}",
        started.elapsed()
    );
}

#[test]
fn backoff_grows_and_caps() {
    let unit = Duration::from_millis(100);
    let delay = |n| next_resolve_delay(n, unit);
    assert!((Duration::from_millis(360)..=Duration::from_millis(400)).contains(&delay(1)));
    assert!((Duration::from_millis(720)..=Duration::from_millis(800)).contains(&delay(2)));
    assert!((Duration::from_millis(23_040)..=Duration::from_millis(25_600)).contains(&delay(7)));
    assert!(delay(50) <= Duration::from_millis(25_600));
}
