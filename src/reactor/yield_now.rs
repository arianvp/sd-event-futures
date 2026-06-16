//! Giving other tasks a turn.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::waker;

/// Future that suspends once, letting every other ready task and the
/// event loop run before its caller continues.
pub struct YieldNow {
    yielded: bool,
}

/// Yield control back to the executor once.
pub fn yield_now() -> YieldNow {
    YieldNow { yielded: false }
}

impl Future for YieldNow {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.yielded {
            Poll::Ready(())
        } else {
            self.yielded = true;
            waker::of(cx).wake_by_ref();
            Poll::Pending
        }
    }
}
