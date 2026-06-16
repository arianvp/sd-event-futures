//! `Gate` tests.
#![deny(unsafe_code)]

mod common;

use std::cell::Cell;
use std::rc::Rc;
use std::time::Duration;

use sd_event_futures::combinator::race;
use sd_event_futures::executor::{block_on, spawn};
use sd_event_futures::reactor::{sleep, yield_now};
use sd_event_futures::sync::Gate;

use common::within;

const LIMIT: Duration = Duration::from_secs(2);

#[test]
fn close_wakes_task_waiting_for_closed() {
    block_on(async {
        let gate = Rc::new(Gate::new());
        let g = gate.clone();
        let waiter = spawn(async move { g.wait_closed().await });
        yield_now().await;
        assert!(!waiter.is_finished());
        gate.close();
        within(LIMIT, waiter).await;
    });
}

#[test]
fn open_wakes_task_waiting_for_open() {
    block_on(async {
        let gate = Rc::new(Gate::new());
        gate.close();
        let g = gate.clone();
        let waiter = spawn(async move { g.wait_open().await });
        yield_now().await;
        assert!(!waiter.is_finished());
        gate.open();
        within(LIMIT, waiter).await;
    });
}

#[test]
fn every_waiter_is_woken() {
    block_on(async {
        let gate = Rc::new(Gate::new());
        let woken = Rc::new(Cell::new(0));
        let waiters: Vec<_> = (0..3)
            .map(|_| {
                let (g, w) = (gate.clone(), woken.clone());
                spawn(async move {
                    g.wait_closed().await;
                    w.set(w.get() + 1);
                })
            })
            .collect();
        yield_now().await;
        gate.close();
        for w in waiters {
            within(LIMIT, w).await;
        }
        assert_eq!(woken.get(), 3);
    });
}

#[test]
fn resolves_immediately_when_already_in_state() {
    block_on(async {
        let gate = Gate::new();
        within(LIMIT, gate.wait_open()).await;
        gate.close();
        within(LIMIT, gate.wait_closed()).await;
    });
}

#[test]
fn dropped_waiters_are_deregistered() {
    block_on(async {
        let gate = Gate::new();
        for _ in 0..10 {
            race(gate.wait_closed(), sleep(Duration::from_millis(1))).await;
        }
        assert_eq!(gate.waiter_count(), 0);
    });
}

#[test]
fn repolling_a_waiter_does_not_grow_the_list() {
    block_on(async {
        let gate = Rc::new(Gate::new());
        let g = gate.clone();
        let waiter = spawn(async move {
            race(g.wait_closed(), async {
                for _ in 0..10 {
                    yield_now().await;
                }
                std::future::pending::<()>().await
            })
            .await;
        });
        for _ in 0..12 {
            yield_now().await;
        }
        assert_eq!(gate.waiter_count(), 1);
        gate.close();
        within(LIMIT, waiter).await;
        assert_eq!(gate.waiter_count(), 0);
    });
}
