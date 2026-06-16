//! Tests of `examples/timesync`, against NTP servers on this machine.
#![deny(unsafe_code)]

mod common;
#[allow(dead_code)]
#[path = "../examples/timesync/main.rs"]
mod timesync;

use std::io::ErrorKind;
use std::net::UdpSocket;
use std::thread;
use std::time::{Duration, Instant, SystemTime};

use sd_event_futures::executor::block_on;

use common::within;
use timesync::{find_time_server, ntp};

const LIMIT: Duration = Duration::from_secs(5);
const REPLY_TIMEOUT: Duration = Duration::from_millis(50);

/// An NTP server whose clock is `ahead` of this machine's, at `stratum`.
fn ntp_server(ahead: Duration, stratum: u8) -> u16 {
    let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    let port = socket.local_addr().unwrap().port();
    thread::spawn(move || {
        let mut request = [0; ntp::PACKET_SIZE];
        while let Ok((_, from)) = socket.recv_from(&mut request) {
            let reply = ntp::reply(&request, stratum, SystemTime::now() + ahead);
            let _ = socket.send_to(&reply, from);
        }
    });
    port
}

/// Bound but never read: requests vanish without an ICMP error.
fn silent_server() -> UdpSocket {
    UdpSocket::bind("127.0.0.1:0").unwrap()
}

#[test]
fn skips_servers_that_do_not_resolve_answer_or_keep_time() {
    let silent = silent_server();
    let unsynchronized = ntp_server(Duration::ZERO, 0);
    let live = ntp_server(Duration::ZERO, 2);
    let servers = [
        ("no-such-host.invalid", ntp::PORT),
        ("127.0.0.1", silent.local_addr().unwrap().port()),
        ("127.0.0.1", unsynchronized),
        ("localhost", live),
    ];

    let started = Instant::now();
    let server = block_on(within(LIMIT, find_time_server(&servers, REPLY_TIMEOUT))).unwrap();
    assert_eq!(server.name, "localhost");
    assert_eq!(server.address.port(), live);
    assert!(
        started.elapsed() >= REPLY_TIMEOUT,
        "silent server wasn't tried"
    );
}

#[test]
fn measures_how_far_the_clock_is_off() {
    let ahead = ntp_server(Duration::from_secs(2), 1);
    let server = block_on(within(
        LIMIT,
        find_time_server(&[("127.0.0.1", ahead)], REPLY_TIMEOUT),
    ))
    .unwrap();
    let sample = server.sample;
    assert!((sample.offset - 2.0).abs() < 0.05, "{sample}");
    assert!((0.0..0.05).contains(&sample.delay), "{sample}");
    assert_eq!(sample.stratum, 1);
}

#[test]
fn reports_when_no_server_answers() {
    let silent = silent_server();
    let servers = [("127.0.0.1", silent.local_addr().unwrap().port())];
    let err = block_on(within(LIMIT, find_time_server(&servers, REPLY_TIMEOUT))).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::NotFound);
}

#[test]
fn rejects_replies_to_another_request() {
    let request = ntp::request(1);
    let reply = ntp::reply(&request, 2, SystemTime::now());
    let now = SystemTime::now();
    let err = ntp::sample(&reply, 2, now, now).unwrap_err();
    assert_eq!(err.to_string(), "Invalid reply; not our transmit time.");
}
