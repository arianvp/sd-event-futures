//! Executor tests: `block_on`, `spawn`, `JoinHandle`.
#![deny(unsafe_code)]
#![cfg_attr(feature = "local-waker", feature(local_waker))]

mod common;

use std::cell::{Cell, RefCell};
use std::future::{Future, poll_fn};
use std::io::Write;
use std::os::unix::net::UnixStream;
use std::panic;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::task::{Context, Poll};
use std::thread;
use std::time::{Duration, Instant};

use sd_event_futures::executor::{JoinHandle, block_on, spawn};
use sd_event_futures::reactor::{AsyncFd, sleep, yield_now};
use sd_event_futures::waker::{self, Waker};

use common::{CountPolls, DropFlag, ThreadSignal, Watchdog, within};

const LIMIT: Duration = Duration::from_secs(2);

struct Immediate(i32);

impl Future for Immediate {
    type Output = i32;
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<i32> {
        Poll::Ready(self.0)
    }
}

#[test]
fn block_on_immediate_ready() {
    assert_eq!(block_on(Immediate(42)), 42);
}

#[test]
fn spawn_and_join_passes_value() {
    let v = block_on(async {
        let h = spawn(async {
            sleep(Duration::from_millis(20)).await;
            7i32
        });
        h.await
    });
    assert_eq!(v, 7);
}

#[test]
fn spawn_runs_tasks_concurrently() {
    let started = Instant::now();
    block_on(async {
        let a = spawn(async {
            sleep(Duration::from_millis(100)).await;
        });
        let b = spawn(async {
            sleep(Duration::from_millis(100)).await;
        });
        a.await;
        b.await;
    });
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_millis(180),
        "tasks ran sequentially? elapsed={elapsed:?}",
    );
    assert!(elapsed >= Duration::from_millis(90));
}

#[test]
fn block_on_returns_when_root_completes_even_with_pending_spawn() {
    let started = Instant::now();
    block_on(async {
        let _detached = spawn(async {
            sleep(Duration::from_secs(60)).await;
        });
    });
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "block_on waited for the detached spawn: {:?}",
        started.elapsed(),
    );
}

#[test]
fn many_spawned_tasks_complete() {
    let total = block_on(async {
        let handles: Vec<_> = (0..16)
            .map(|i| {
                spawn(async move {
                    yield_now().await;
                    i
                })
            })
            .collect();
        let mut sum = 0;
        for h in handles {
            sum += h.await;
        }
        sum
    });
    assert_eq!(total, (0..16).sum::<i32>());
}

#[test]
fn spawn_inside_spawned_task() {
    let v = block_on(async {
        let outer = spawn(async {
            let inner = spawn(async { 41 });
            inner.await + 1
        });
        outer.await
    });
    assert_eq!(v, 42);
}

// =============================================================================
// Wake bookkeeping
// =============================================================================

/// Wakes itself twice during its first poll, then stays pending.
struct WakeTwiceOnFirstPoll {
    polls: Rc<Cell<u32>>,
}

impl Future for WakeTwiceOnFirstPoll {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        self.polls.set(self.polls.get() + 1);
        if self.polls.get() == 1 {
            waker::of(cx).wake_by_ref();
            waker::of(cx).wake_by_ref();
        }
        Poll::Pending
    }
}

#[test]
fn repeated_wakes_queue_a_task_once() {
    let polls = Rc::new(Cell::new(0));
    block_on({
        let polls = polls.clone();
        async move {
            let task = spawn(WakeTwiceOnFirstPoll { polls });
            sleep(Duration::from_millis(20)).await;
            task.abort();
        }
    });
    assert_eq!(polls.get(), 2);
}

/// Panics raised on the calling thread while running `f`, including ones
/// that something caught.
fn panics_during(f: impl FnOnce()) -> u32 {
    static PANICS: AtomicU32 = AtomicU32::new(0);
    let me = thread::current().id();
    let prev = Arc::new(panic::take_hook());
    let hook_prev = prev.clone();
    panic::set_hook(Box::new(move |info| {
        if thread::current().id() == me {
            PANICS.fetch_add(1, Ordering::SeqCst);
        }
        hook_prev(info);
    }));
    let before = PANICS.load(Ordering::SeqCst);
    f();
    let after = PANICS.load(Ordering::SeqCst);
    let _ = panic::take_hook();
    after - before
}

#[test]
fn waking_a_finished_task_is_ignored() {
    let panics = panics_during(|| {
        block_on(async {
            let stash: Rc<RefCell<Option<Waker>>> = Rc::default();
            let s = stash.clone();
            let task = spawn(poll_fn(move |cx| {
                *s.borrow_mut() = Some(waker::of(cx).clone());
                Poll::Ready(5)
            }));
            assert_eq!(within(LIMIT, task).await, 5);

            stash.borrow_mut().take().unwrap().wake();
            sleep(Duration::from_millis(10)).await;
        });
    });
    assert_eq!(panics, 0);
}

// =============================================================================
// Panics
// =============================================================================

#[test]
fn panic_in_task_is_raised_by_join_handle() {
    let err = panic::catch_unwind(|| {
        block_on(async {
            let task = spawn(async {
                if true {
                    panic!("boom");
                }
                1
            });
            within(LIMIT, task).await
        })
    })
    .unwrap_err();
    assert_eq!(err.downcast_ref::<&str>(), Some(&"boom"));
}

#[test]
fn panic_in_task_does_not_stall_other_tasks() {
    let sum = block_on(async {
        let _bad = spawn(async {
            yield_now().await;
            panic!("boom");
        });
        let good: Vec<_> = (1..=3)
            .map(|i| {
                spawn(async move {
                    yield_now().await;
                    i
                })
            })
            .collect();
        let mut sum = 0;
        for h in good {
            sum += within(LIMIT, h).await;
        }
        sum
    });
    assert_eq!(sum, 6);
}

// =============================================================================
// JoinHandle
// =============================================================================

#[test]
#[should_panic(expected = "JoinHandle polled after completion")]
fn join_handle_polled_after_completion_panics() {
    block_on(async {
        let mut h = spawn(async { 1 });
        assert_eq!((&mut h).await, 1);
        (&mut h).await;
    });
}

#[test]
fn is_finished_tracks_completion() {
    block_on(async {
        let h = spawn(sleep(Duration::from_millis(10)));
        assert!(!h.is_finished());
        sleep(Duration::from_millis(30)).await;
        assert!(h.is_finished());
        h.await;
    });
}

#[test]
fn abort_drops_task_immediately() {
    block_on(async {
        let dropped = Rc::new(Cell::new(false));
        let d = dropped.clone();
        let h = spawn(async move {
            let _flag = DropFlag(d);
            sleep(Duration::from_secs(60)).await;
        });
        yield_now().await;
        assert!(!dropped.get());
        h.abort();
        assert!(dropped.get());
    });
}

#[test]
fn task_can_abort_itself() {
    block_on(async {
        let dropped = Rc::new(Cell::new(false));
        let own: Rc<RefCell<Option<JoinHandle<()>>>> = Rc::default();
        let (d, o) = (dropped.clone(), own.clone());
        let h = spawn(async move {
            let _flag = DropFlag(d);
            yield_now().await;
            o.borrow_mut().take().unwrap().abort();
            sleep(Duration::from_secs(60)).await;
        });
        *own.borrow_mut() = Some(h);
        within(LIMIT, async {
            while !dropped.get() {
                yield_now().await;
            }
        })
        .await;
    });
}

// =============================================================================
// Thread-safe wakers
// =============================================================================

#[test]
#[cfg_attr(
    feature = "local-waker",
    should_panic(expected = "only supports LocalWaker")
)]
fn waker_from_another_thread_wakes_spawned_task() {
    block_on(async {
        let sig = ThreadSignal::default();
        sig.fire_after(Duration::from_millis(20));
        within(LIMIT, spawn(sig)).await;
    });
}

#[test]
#[cfg_attr(
    feature = "local-waker",
    should_panic(expected = "only supports LocalWaker")
)]
fn waker_from_another_thread_wakes_root() {
    let start = Instant::now();
    block_on(async {
        let sig = ThreadSignal::default();
        sig.fire_after(Duration::from_millis(20));
        within(LIMIT, sig).await;
    });
    assert!(
        start.elapsed() < LIMIT / 2,
        "woken late: {:?}",
        start.elapsed()
    );
}

// =============================================================================
// Task ownership
// =============================================================================

#[test]
fn unfinished_tasks_are_dropped_when_block_on_returns() {
    let dropped = Rc::new(Cell::new(false));
    let d = dropped.clone();
    block_on(async move {
        let _detached = spawn(async move {
            let _flag = DropFlag(d);
            sleep(Duration::from_secs(60)).await;
        });
        yield_now().await;
    });
    assert!(dropped.get());
}

#[test]
fn tasks_do_not_run_after_their_block_on() {
    let ran = Rc::new(Cell::new(false));
    let r = ran.clone();
    block_on(async move {
        let _detached = spawn(async move {
            sleep(Duration::from_millis(20)).await;
            r.set(true);
        });
        yield_now().await;
    });
    block_on(sleep(Duration::from_millis(60)));
    assert!(!ran.get());
}

// =============================================================================
// Fairness
// =============================================================================

/// Always pending, always immediately ready again.
struct Spin;

impl Future for Spin {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        waker::of(cx).wake_by_ref();
        Poll::Pending
    }
}

#[test]
fn self_waking_task_does_not_starve_timers() {
    let _watchdog = Watchdog::arm("self_waking_task_does_not_starve_timers", LIMIT * 2);
    block_on(async {
        let spin = spawn(Spin);
        within(LIMIT, sleep(Duration::from_millis(20))).await;
        spin.abort();
    });
}

#[test]
fn self_waking_task_does_not_starve_io() {
    let _watchdog = Watchdog::arm("self_waking_task_does_not_starve_io", LIMIT * 2);
    let (a, mut b) = UnixStream::pair().unwrap();
    b.write_all(b"x").unwrap();
    block_on(async move {
        let spin = spawn(Spin);
        let a = AsyncFd::new(a).unwrap();
        within(LIMIT, a.readable()).await;
        spin.abort();
    });
}

#[test]
fn root_is_polled_only_when_woken() {
    let polls = Rc::new(Cell::new(0));
    block_on(CountPolls {
        inner: Box::pin(async {
            let busy = spawn(async {
                for _ in 0..50 {
                    sleep(Duration::from_millis(1)).await;
                }
            });
            busy.await;
        }),
        polls: polls.clone(),
    });
    assert!(polls.get() <= 3, "root polled {} times", polls.get());
}

// =============================================================================
// spawn_blocking
// =============================================================================

#[cfg(not(feature = "local-waker"))]
mod blocking {
    use super::*;
    use sd_event_futures::executor::spawn_blocking;

    fn sigterm_blocked() -> bool {
        sd_event_futures::os::is_blocked(libc::SIGTERM)
    }

    #[test]
    fn spawn_blocking_returns_value_without_blocking_loop() {
        block_on(async {
            let slow = spawn_blocking(|| {
                thread::sleep(Duration::from_millis(50));
                42
            });
            let ticker = spawn(async {
                let mut ticks = 0;
                for _ in 0..5 {
                    sleep(Duration::from_millis(2)).await;
                    ticks += 1;
                }
                ticks
            });
            assert_eq!(within(LIMIT, ticker).await, 5);
            assert_eq!(within(LIMIT, slow).await, 42);
        });
    }

    #[test]
    fn spawn_blocking_reraises_panic() {
        let err = panic::catch_unwind(|| {
            block_on(within(
                LIMIT,
                spawn_blocking(|| -> u32 { panic!("blocking boom") }),
            ))
        })
        .unwrap_err();
        assert_eq!(err.downcast_ref::<&str>(), Some(&"blocking boom"));
    }

    /// Pool threads alive right now, from their thread names.
    fn pool_threads() -> usize {
        std::fs::read_dir("/proc/self/task")
            .unwrap()
            .filter_map(|t| std::fs::read_to_string(t.ok()?.path().join("comm")).ok())
            .filter(|comm| comm.trim_end() == "blocking")
            .count()
    }

    #[test]
    fn blocking_pool_caps_threads_and_queues_the_rest() {
        use sd_event_futures::blocking::MAX_THREADS;
        use sd_event_futures::combinator::join_all;

        let jobs = MAX_THREADS * 3;
        let started = Instant::now();
        let seen = block_on(within(
            LIMIT,
            join_all((0..jobs).map(|_| {
                spawn_blocking(|| {
                    thread::sleep(Duration::from_millis(50));
                    pool_threads()
                })
            })),
        ));
        let most = seen.into_iter().max().unwrap();
        assert!(most <= MAX_THREADS, "{most} pool threads");
        // Three jobs per thread run one after another.
        assert!(
            started.elapsed() >= Duration::from_millis(150),
            "{:?}",
            started.elapsed()
        );
    }

    #[test]
    fn blocking_pool_reuses_threads() {
        use sd_event_futures::blocking::MAX_THREADS;
        use std::collections::HashSet;

        let threads: HashSet<_> = block_on(async {
            let mut ids = HashSet::new();
            for _ in 0..100 {
                ids.insert(within(LIMIT, spawn_blocking(|| thread::current().id())).await);
            }
            ids
        });
        assert!(
            threads.len() <= MAX_THREADS,
            "{} threads for 100 jobs",
            threads.len()
        );
    }

    #[test]
    fn spawn_blocking_blocks_signals_in_worker_only() {
        let before = sigterm_blocked();
        let in_worker = block_on(within(LIMIT, spawn_blocking(sigterm_blocked)));
        assert!(in_worker, "worker could steal signals meant for the loop");
        assert_eq!(sigterm_blocked(), before, "caller's signal mask changed");
    }
}
