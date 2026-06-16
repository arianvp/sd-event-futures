//! `race`, `join_all` and `select!` tests.
#![deny(unsafe_code)]
#![cfg_attr(feature = "local-waker", feature(local_waker))]

mod common;

use std::cell::{Cell, RefCell};
use std::future::{Future, pending, poll_fn, ready};
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use sd_event_futures::combinator::{Either, join_all, race};
use sd_event_futures::executor::block_on;
use sd_event_futures::reactor::{sleep, yield_now};
use sd_event_futures::select;

use common::{CountPolls, DropFlag, within};

const LIMIT: Duration = Duration::from_secs(2);

#[test]
fn race_resolves_with_first_and_drops_loser() {
    let dropped = Rc::new(Cell::new(false));
    let d = dropped.clone();
    let winner = block_on(race(sleep(Duration::from_millis(10)), async move {
        let _flag = DropFlag(d);
        pending::<()>().await
    }));
    assert!(matches!(winner, Either::Left(())));
    assert!(dropped.get());
}

#[test]
fn race_prefers_left_on_tie() {
    assert!(matches!(
        block_on(race(ready(1), ready(2))),
        Either::Left(1)
    ));
}

#[test]
fn join_all_keeps_input_order() {
    // Later futures finish first.
    let out = block_on(within(
        LIMIT,
        join_all((0..5u64).map(|i| async move {
            sleep(Duration::from_millis(50 - 10 * i)).await;
            i
        })),
    ));
    assert_eq!(out, [0, 1, 2, 3, 4]);
}

#[test]
fn join_all_runs_futures_concurrently() {
    let started = Instant::now();
    block_on(within(
        LIMIT,
        join_all((0..10).map(|_| sleep(Duration::from_millis(50)))),
    ));
    let elapsed = started.elapsed();
    assert!(elapsed >= Duration::from_millis(50));
    assert!(
        elapsed < Duration::from_millis(250),
        "ran one after another: {elapsed:?}"
    );
}

#[test]
fn join_all_of_nothing_is_ready_immediately() {
    let out: Vec<()> = block_on(join_all(std::iter::empty::<std::future::Ready<()>>()));
    assert!(out.is_empty());
}

#[test]
#[should_panic(expected = "polled after completion")]
fn join_all_polled_after_completion_panics() {
    block_on(async {
        let mut all = join_all([ready(1)]);
        assert_eq!((&mut all).await, [1]);
        (&mut all).await;
    });
}

/// Ready after being polled `n` times, waking itself in between.
struct ReadyAfter(u32);

impl Future for ReadyAfter {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.0 == 0 {
            return Poll::Ready(());
        }
        self.0 -= 1;
        sd_event_futures::waker::of(cx).wake_by_ref();
        Poll::Pending
    }
}

#[test]
fn join_all_only_repolls_futures_that_woke() {
    let busy = Rc::new(Cell::new(0));
    let idle: Vec<Rc<Cell<u32>>> = (0..5).map(|_| Rc::default()).collect();
    block_on(within(LIMIT, async {
        let gate = Rc::new(sd_event_futures::sync::Gate::new());
        gate.close();
        let mut futures: Vec<Pin<Box<dyn Future<Output = ()>>>> = vec![Box::pin(CountPolls {
            inner: Box::pin(ReadyAfter(20)),
            polls: busy.clone(),
        })];
        for polls in &idle {
            let gate = gate.clone();
            futures.push(Box::pin(CountPolls {
                inner: Box::pin(async move { gate.wait_open().await }),
                polls: polls.clone(),
            }));
        }
        let all = join_all(futures);
        let opener = async {
            // Let the busy future finish its 20 wakes first.
            for _ in 0..30 {
                yield_now().await;
            }
            gate.open();
        };
        sd_event_futures::combinator::race(all, async {
            opener.await;
            pending::<()>().await
        })
        .await;
    }));
    assert_eq!(busy.get(), 21);
    for polls in &idle {
        assert_eq!(polls.get(), 2, "polled on every wake of its sibling");
    }
}

#[test]
fn select_runs_the_first_ready_branch() {
    let picked = block_on(async {
        select! {
            () = sleep(Duration::from_millis(50)) => "slow",
            () = sleep(Duration::from_millis(5)) => "fast",
        }
    });
    assert_eq!(picked, "fast");
}

#[test]
fn select_prefers_earlier_branches_when_several_are_ready() {
    let picked = block_on(async {
        select! {
            a = ready(1) => a,
            b = ready(2) => b,
        }
    });
    assert_eq!(picked, 1);
}

#[test]
fn select_drops_losers_before_running_the_branch() {
    let dropped = Rc::new(Cell::new(false));
    let d = dropped.clone();
    let seen = block_on(async {
        select! {
            () = async move {
                let _flag = DropFlag(d);
                pending::<()>().await
            } => unreachable!(),
            () = ready(()) => dropped.get(),
        }
    });
    assert!(seen, "loser still alive while the branch ran");
}

#[test]
fn select_branches_can_use_what_the_futures_borrowed() {
    let log = RefCell::new(Vec::new());
    block_on(async {
        let mut counter = 0;
        for _ in 0..3 {
            select! {
                () = poll_fn(|_| { counter += 1; Poll::Ready(()) }) => log.borrow_mut().push(counter),
                () = pending::<()>() => unreachable!(),
            }
        }
    });
    assert_eq!(*log.borrow(), [1, 2, 3]);
}

#[test]
fn select_branches_control_the_enclosing_loop() {
    let ticks = block_on(async {
        let mut ticks = 0;
        loop {
            select! {
                () = sleep(Duration::from_millis(1)) => {
                    ticks += 1;
                    if ticks < 3 {
                        continue;
                    }
                    break ticks;
                },
                () = pending::<()>() => unreachable!(),
            }
        }
    });
    assert_eq!(ticks, 3);
}
