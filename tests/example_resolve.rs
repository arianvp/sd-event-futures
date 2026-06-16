//! Tests of `examples/resolve.rs`.
#![deny(unsafe_code)]

mod common;
#[allow(dead_code)]
#[path = "../examples/resolve.rs"]
mod resolve;

use std::time::Duration;

use sd_event_futures::combinator::join_all;
use sd_event_futures::executor::block_on;

use common::within;
use resolve::lookup;

const LIMIT: Duration = Duration::from_secs(5);

#[test]
fn looks_up_names_concurrently() {
    let answers = block_on(within(LIMIT, join_all((0..4).map(|_| lookup("localhost")))));
    for addresses in answers {
        let addresses = addresses.expect("localhost resolves");
        assert!(addresses.iter().any(|a| a.is_loopback()), "{addresses:?}");
    }
}

#[test]
fn reports_names_that_do_not_resolve() {
    block_on(within(LIMIT, lookup("no-such-host.invalid"))).unwrap_err();
}
