//! Executor micro-benchmarks. Compare the two executors with
//!
//! ```sh
//! cargo bench
//! cargo bench --features local-waker
//! ```
#![forbid(unsafe_code)]
#![cfg_attr(feature = "local-waker", feature(local_waker))]

use std::cell::{Cell, RefCell};
use std::future::poll_fn;
use std::hint::black_box;
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::rc::Rc;
use std::task::Poll;
use std::time::{Duration, Instant};

use sd_event_futures::executor::{block_on, spawn};
use sd_event_futures::reactor::{AsyncFd, yield_now};
use sd_event_futures::waker::{self, Waker};

const RUNS: usize = 7;

/// Single-slot notification: the smallest primitive that parks a waker.
#[derive(Default)]
struct Notify {
    set: Cell<bool>,
    waker: RefCell<Option<Waker>>,
}

impl Notify {
    fn notify(&self) {
        self.set.set(true);
        let w = self.waker.borrow_mut().take();
        if let Some(w) = w {
            w.wake();
        }
    }

    async fn wait(&self, clone_every_time: bool) {
        poll_fn(|cx| {
            if self.set.replace(false) {
                return Poll::Ready(());
            }
            let w = waker::of(cx);
            let mut slot = self.waker.borrow_mut();
            match &mut *slot {
                Some(old) if !clone_every_time && old.will_wake(w) => {}
                _ => *slot = Some(w.clone()),
            }
            Poll::Pending
        })
        .await
    }
}

fn spawn_join(n: u64) {
    block_on(async move {
        let handles: Vec<_> = (0..n).map(|i| spawn(async move { i })).collect();
        let mut sum = 0;
        for h in handles {
            sum += h.await;
        }
        black_box(sum);
    });
}

fn yield_batch(tasks: u64, yields: u64) {
    block_on(async move {
        let handles: Vec<_> = (0..tasks)
            .map(|_| {
                spawn(async move {
                    for _ in 0..yields {
                        yield_now().await;
                    }
                })
            })
            .collect();
        for h in handles {
            h.await;
        }
    });
}

fn ping_pong(pairs: u64, rounds: u64, clone_every_time: bool) {
    block_on(async move {
        let mut handles = Vec::new();
        for _ in 0..pairs {
            let ping = Rc::new(Notify::default());
            let pong = Rc::new(Notify::default());
            let (pi, po) = (ping.clone(), pong.clone());
            handles.push(spawn(async move {
                for _ in 0..rounds {
                    pi.wait(clone_every_time).await;
                    po.notify();
                }
            }));
            handles.push(spawn(async move {
                for _ in 0..rounds {
                    ping.notify();
                    pong.wait(clone_every_time).await;
                }
            }));
        }
        for h in handles {
            h.await;
        }
    });
}

fn waker_clone(n: u64) {
    block_on(poll_fn(move |cx| {
        let w = waker::of(cx);
        for _ in 0..n {
            drop(black_box(w.clone()));
        }
        Poll::Ready(())
    }));
}

fn io_echo(rounds: u64) {
    let (a, b) = UnixStream::pair().unwrap();
    block_on(async move {
        let a = AsyncFd::new(a).unwrap();
        let b = AsyncFd::new(b).unwrap();
        let echo = spawn(async move {
            let mut buf = [0u8; 1];
            for _ in 0..rounds {
                b.read_with(|mut s| s.read_exact(&mut buf)).await.unwrap();
                b.write_with(|mut s| s.write_all(&buf)).await.unwrap();
            }
        });
        let mut buf = [0u8; 1];
        for _ in 0..rounds {
            a.write_with(|mut s| s.write_all(b"x")).await.unwrap();
            a.read_with(|mut s| s.read_exact(&mut buf)).await.unwrap();
        }
        echo.await;
    });
}

fn bench(name: &str, ops: u64, unit: &str, f: impl Fn()) {
    let mut times: Vec<Duration> = (0..RUNS)
        .map(|_| {
            let start = Instant::now();
            f();
            start.elapsed()
        })
        .collect();
    times.sort();
    let median = times[RUNS / 2];
    let per_op = median.as_nanos() as f64 / ops as f64;
    println!("{name:<32} {per_op:>9.1} ns/{unit}   (median of {RUNS}, {median:.2?} total)");
}

fn main() {
    let executor = if cfg!(feature = "local-waker") {
        "LocalWaker"
    } else {
        "Waker"
    };
    println!("executor: {executor}");
    bench("waker clone+drop", 10_000_000, "clone", || {
        waker_clone(10_000_000)
    });
    bench("spawn + join", 200_000, "task", || spawn_join(200_000));
    bench("yield (1000 tasks x 1000)", 1_000_000, "yield", || {
        yield_batch(1000, 1000)
    });
    bench("ping-pong (1000 pairs x 1000)", 2_000_000, "wake", || {
        ping_pong(1000, 1000, false)
    });
    bench("ping-pong, clone per wait", 2_000_000, "wake", || {
        ping_pong(1000, 1000, true)
    });
    bench("ping-pong (1 pair x 100k)", 200_000, "wake", || {
        ping_pong(1, 100_000, false)
    });
    bench("socket echo (100k round trips)", 100_000, "trip", || {
        io_echo(100_000)
    });
}
