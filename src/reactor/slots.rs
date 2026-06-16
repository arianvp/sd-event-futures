//! Where source callbacks leave results and wakers for the futures that
//! await them.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::waker::{self, Waker};

pub(super) fn register(slot: &RefCell<Option<Waker>>, waker: &Waker) {
    let mut slot = slot.borrow_mut();
    match &mut *slot {
        Some(w) if w.will_wake(waker) => {}
        Some(w) => w.clone_from(waker),
        None => *slot = Some(waker.clone()),
    }
}

pub(super) fn wake(slot: &RefCell<Option<Waker>>) {
    let waker = slot.borrow_mut().take();
    if let Some(w) = waker {
        w.wake();
    }
}

/// Result of a one-shot source, plus the waker of whoever awaits it.
pub(super) struct WakeSlot<T> {
    value: RefCell<Option<T>>,
    waker: RefCell<Option<Waker>>,
}

impl<T> Default for WakeSlot<T> {
    fn default() -> Self {
        Self {
            value: RefCell::new(None),
            waker: RefCell::new(None),
        }
    }
}

impl<T> WakeSlot<T> {
    pub(super) fn complete(&self, value: T) {
        *self.value.borrow_mut() = Some(value);
        wake(&self.waker);
    }

    pub(super) fn poll(&self, cx: &mut Context<'_>) -> Poll<T> {
        if let Some(v) = self.value.borrow_mut().take() {
            Poll::Ready(v)
        } else {
            register(&self.waker, waker::of(cx));
            Poll::Pending
        }
    }
}

/// Events delivered by a long-lived source, waiting to be consumed.
pub(super) struct Queue<T> {
    items: RefCell<VecDeque<T>>,
    waker: RefCell<Option<Waker>>,
}

impl<T> Default for Queue<T> {
    fn default() -> Self {
        Self {
            items: RefCell::new(VecDeque::new()),
            waker: RefCell::new(None),
        }
    }
}

impl<T> Queue<T> {
    pub(super) fn push(&self, item: T) {
        self.items.borrow_mut().push_back(item);
        wake(&self.waker);
    }

    /// A future for the next item.
    pub(super) fn recv(&self) -> Recv<'_, T> {
        Recv { queue: self }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.items.borrow().is_empty()
    }

    pub(super) fn poll_pop(&self, cx: &mut Context<'_>) -> Poll<T> {
        if let Some(v) = self.items.borrow_mut().pop_front() {
            Poll::Ready(v)
        } else {
            register(&self.waker, waker::of(cx));
            Poll::Pending
        }
    }
}

/// Future that resolves with the next queued event of a [`Signal`](super::Signal),
/// [`Inotify`](super::Inotify) or [`MemoryPressure`](super::MemoryPressure) handle.
pub struct Recv<'a, T> {
    queue: &'a Queue<T>,
}

impl<T> Future for Recv<'_, T> {
    type Output = T;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        self.queue.poll_pop(cx)
    }
}
