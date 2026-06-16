//! Single-threaded synchronization primitives: [`Gate`] and [`TaskTracker`].

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use crate::waker::{self, Waker};

/// A set of parked wakers. Each waiting future owns a key, refreshes its
/// waker on every `Pending` poll, and deregisters on drop, so the list
/// only ever holds wakers of futures that still exist.
#[derive(Default)]
pub(crate) struct WaitList {
    waiters: RefCell<HashMap<u64, Waker>>,
    next_key: Cell<u64>,
}

impl WaitList {
    pub(crate) fn register(&self, key: &mut Option<u64>, waker: &Waker) {
        let key = *key.get_or_insert_with(|| {
            let k = self.next_key.get();
            self.next_key.set(k + 1);
            k
        });
        let mut waiters = self.waiters.borrow_mut();
        match waiters.get_mut(&key) {
            Some(w) if w.will_wake(waker) => {}
            Some(w) => w.clone_from(waker),
            None => {
                waiters.insert(key, waker.clone());
            }
        }
    }

    pub(crate) fn deregister(&self, key: Option<u64>) {
        if let Some(key) = key {
            self.waiters.borrow_mut().remove(&key);
        }
    }

    pub(crate) fn wake_all(&self) {
        let waiters = std::mem::take(&mut *self.waiters.borrow_mut());
        for (_, w) in waiters {
            w.wake();
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.waiters.borrow().len()
    }
}

/// A flag that tasks can wait on in either direction.
///
/// Every transition wakes all tasks waiting for the new state.
#[derive(Default)]
pub struct Gate {
    closed: Cell<bool>,
    open_waiters: WaitList,
    closed_waiters: WaitList,
}

impl Gate {
    /// An open gate.
    pub fn new() -> Self {
        Self::default()
    }

    /// A closed gate.
    pub fn closed() -> Self {
        let gate = Self::new();
        gate.closed.set(true);
        gate
    }

    pub fn close(&self) {
        if !self.closed.replace(true) {
            self.closed_waiters.wake_all();
        }
    }

    pub fn open(&self) {
        if self.closed.replace(false) {
            self.open_waiters.wake_all();
        }
    }

    pub fn is_closed(&self) -> bool {
        self.closed.get()
    }

    /// Resolves once the gate is open (immediately if it already is).
    pub fn wait_open(&self) -> WaitFor<'_> {
        WaitFor {
            gate: self,
            closed: false,
            key: None,
        }
    }

    /// Resolves once the gate is closed (immediately if it already is).
    pub fn wait_closed(&self) -> WaitFor<'_> {
        WaitFor {
            gate: self,
            closed: true,
            key: None,
        }
    }

    /// Number of currently parked waiters, in both directions.
    pub fn waiter_count(&self) -> usize {
        self.open_waiters.len() + self.closed_waiters.len()
    }

    fn waiters(&self, closed: bool) -> &WaitList {
        if closed {
            &self.closed_waiters
        } else {
            &self.open_waiters
        }
    }
}

/// Future returned by [`Gate::wait_open`] and [`Gate::wait_closed`].
pub struct WaitFor<'a> {
    gate: &'a Gate,
    closed: bool,
    key: Option<u64>,
}

impl Future for WaitFor<'_> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        if this.gate.closed.get() == this.closed {
            this.gate.waiters(this.closed).deregister(this.key.take());
            Poll::Ready(())
        } else {
            this.gate
                .waiters(this.closed)
                .register(&mut this.key, waker::of(cx));
            Poll::Pending
        }
    }
}

impl Drop for WaitFor<'_> {
    fn drop(&mut self) {
        self.gate.waiters(self.closed).deregister(self.key);
    }
}

/// Counts tasks that are still running, so that others can wait until
/// none are: open connections before a shutdown, for example.
#[derive(Default)]
pub struct TaskTracker {
    running: Cell<usize>,
    /// Open while nothing runs.
    idle: Gate,
}

impl TaskTracker {
    pub fn new() -> Rc<Self> {
        Rc::default()
    }

    /// `future`, counted as running from now until it finishes or is
    /// dropped.
    pub fn track<F: Future>(
        self: &Rc<Self>,
        future: F,
    ) -> impl Future<Output = F::Output> + use<F> {
        let running = Running::new(self.clone());
        async move {
            let _running = running;
            future.await
        }
    }

    pub fn len(&self) -> usize {
        self.running.get()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Open while no tracked task runs, closed while any does.
    pub fn idle(&self) -> &Gate {
        &self.idle
    }
}

/// One tracked task, counted until dropped.
struct Running(Rc<TaskTracker>);

impl Running {
    fn new(tracker: Rc<TaskTracker>) -> Self {
        tracker.running.set(tracker.running.get() + 1);
        tracker.idle.close();
        Self(tracker)
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        let tracker = &self.0;
        tracker.running.set(tracker.running.get() - 1);
        if tracker.is_empty() {
            tracker.idle.open();
        }
    }
}
