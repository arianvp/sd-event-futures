//! Child processes changing state.

use std::future::Future;
use std::io;
use std::os::raw::c_int;
use std::os::unix::io::RawFd;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use libc::{pid_t, siginfo_t};

use super::current_event;
use super::slots::WakeSlot;
use crate::EventSource;

/// Future that resolves to the child's `siginfo_t` on the next watched
/// state change.
pub struct WaitChild {
    slot: Rc<WakeSlot<siginfo_t>>,
    _src: EventSource,
}

/// Wait for `pid` to undergo one of the state transitions in
/// `options` (e.g. `libc::WEXITED`).
///
/// The loop reaps the child right after reporting a `WEXITED` change.
/// Kernel autoreap must be off for `pid` (SIGCHLD not ignored), and when
/// watching `WSTOPPED` or `WCONTINUED`, SIGCHLD must be blocked in all
/// threads; see `sd_event_add_child(3)`. [`crate::process::Child`] wraps
/// this for processes it forks.
pub fn child(pid: pid_t, options: c_int) -> io::Result<WaitChild> {
    let slot = Rc::new(WakeSlot::default());
    let s = slot.clone();
    let src = current_event()?.add_child(pid, options, move |si| {
        s.complete(*si);
        0
    })?;
    Ok(WaitChild { slot, _src: src })
}

/// Like [`child`] but watches a pidfd (see `pidfd_open(2)`).
pub fn child_pidfd(pidfd: RawFd, options: c_int) -> io::Result<WaitChild> {
    let slot = Rc::new(WakeSlot::default());
    let s = slot.clone();
    let src = current_event()?.add_child_pidfd(pidfd, options, move |si| {
        s.complete(*si);
        0
    })?;
    Ok(WaitChild { slot, _src: src })
}

impl Future for WaitChild {
    type Output = siginfo_t;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<siginfo_t> {
        self.slot.poll(cx)
    }
}
