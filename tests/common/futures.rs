//! Futures for driving tests: a timeout, a waker from another thread,
//! poll counting and drop detection.

use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use sd_event_futures::combinator::{Either, race};
use sd_event_futures::reactor::sleep;

/// Await `fut`, failing the test instead of hanging if it takes longer
/// than `limit`. Must be awaited inside `block_on`. The deadline is checked
/// first, so a future that only turns out ready once the timer fires
/// (because nothing woke it) still fails.
pub async fn within<F: Future>(limit: Duration, fut: F) -> F::Output {
    match race(sleep(limit), fut).await {
        Either::Left(()) => panic!("timed out after {limit:?}"),
        Either::Right(v) => v,
    }
}

/// A one-shot completion signalled from another thread through the
/// standard `Waker`, the way most foreign futures (channels, thread
/// pools) report readiness.
#[derive(Clone, Default)]
pub struct ThreadSignal(Arc<Mutex<(bool, Option<Waker>)>>);

impl ThreadSignal {
    pub fn fire_after(&self, delay: Duration) {
        let this = self.clone();
        std::thread::spawn(move || {
            std::thread::sleep(delay);
            let waker = {
                let mut s = this.0.lock().unwrap();
                s.0 = true;
                s.1.take()
            };
            if let Some(w) = waker {
                w.wake();
            }
        });
    }
}

impl Future for ThreadSignal {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let mut s = self.0.lock().unwrap();
        if s.0 {
            Poll::Ready(())
        } else {
            s.1 = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

/// Counts how often the wrapped future is polled.
pub struct CountPolls<F> {
    pub inner: Pin<Box<F>>,
    pub polls: Rc<Cell<u32>>,
}

impl<F: Future> Future for CountPolls<F> {
    type Output = F::Output;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        self.polls.set(self.polls.get() + 1);
        self.inner.as_mut().poll(cx)
    }
}

/// Sets its flag when dropped.
pub struct DropFlag(pub Rc<Cell<bool>>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.set(true);
    }
}

/// Exits the test process if still armed after `limit`. For tests whose
/// failure mode is the event loop never getting a chance to run, where
/// an in-loop timeout like [`within`] can't fire either.
pub struct Watchdog(Arc<AtomicBool>);

impl Watchdog {
    pub fn arm(name: &'static str, limit: Duration) -> Self {
        let armed = Arc::new(AtomicBool::new(true));
        let a = armed.clone();
        std::thread::spawn(move || {
            std::thread::sleep(limit);
            if a.load(Ordering::SeqCst) {
                eprintln!("{name}: event loop starved for {limit:?}");
                std::process::exit(101);
            }
        });
        Self(armed)
    }
}

impl Drop for Watchdog {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}
