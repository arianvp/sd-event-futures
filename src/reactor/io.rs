//! File descriptors registered with the loop once, for their whole
//! lifetime.

use std::cell::Cell;
use std::future::Future;
use std::io;
use std::os::fd::{AsFd, AsRawFd};
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use super::current_event;
use crate::sync::WaitList;
use crate::waker;
use crate::{EventSource, os};

const READ_READY: u32 = (libc::EPOLLIN | libc::EPOLLPRI) as u32;
const WRITE_READY: u32 = libc::EPOLLOUT as u32;
/// Conditions that persist once reported. Edge-triggered epoll does not
/// report them again, so clearing readiness keeps these bits.
const CLOSED: u32 = (libc::EPOLLHUP | libc::EPOLLERR | libc::EPOLLRDHUP) as u32;

#[derive(Default)]
struct IoState {
    ready: Cell<u32>,
    readers: WaitList,
    writers: WaitList,
}

impl IoState {
    fn is_ready(&self, write: bool) -> bool {
        let mask = if write {
            WRITE_READY | (libc::EPOLLHUP | libc::EPOLLERR) as u32
        } else {
            READ_READY | CLOSED
        };
        self.ready.get() & mask != 0
    }

    fn waiters(&self, write: bool) -> &WaitList {
        if write { &self.writers } else { &self.readers }
    }
}

/// An fd registered with the event loop for its whole lifetime.
///
/// The fd is switched to non-blocking mode and watched edge-triggered
/// for both directions. Readiness is cached until an operation reports
/// `WouldBlock`; [`AsyncFd::read_with`] and [`AsyncFd::write_with`]
/// handle that bookkeeping.
///
/// Only one `AsyncFd` may exist per fd: epoll rejects a second
/// registration with `EEXIST`.
pub struct AsyncFd<T: AsFd> {
    // Declared before `inner` so the fd is removed from epoll before it
    // is closed.
    src: EventSource,
    state: Rc<IoState>,
    inner: T,
}

impl<T: AsFd> AsyncFd<T> {
    /// Register `inner` with the current thread's event loop.
    ///
    /// Fails like `epoll_ctl(2)`, e.g. with `EPERM` for a regular file or
    /// `EEXIST` if the fd is already registered.
    pub fn new(inner: T) -> io::Result<Self> {
        let fd = inner.as_fd().as_raw_fd();
        os::set_nonblocking(inner.as_fd())?;
        let state = Rc::new(IoState::default());
        let s = state.clone();
        let events = (libc::EPOLLIN | libc::EPOLLOUT | libc::EPOLLRDHUP | libc::EPOLLET) as u32;
        let src = current_event()?.add_io(fd, events, move |_, revents| {
            s.ready.set(s.ready.get() | revents);
            if s.is_ready(false) {
                s.readers.wake_all();
            }
            if s.is_ready(true) {
                s.writers.wake_all();
            }
            0
        })?;
        Ok(Self { src, state, inner })
    }

    pub fn get_ref(&self) -> &T {
        &self.inner
    }

    /// Deregister the fd and return the wrapped value.
    pub fn into_inner(self) -> T {
        let Self { src, inner, .. } = self;
        drop(src);
        inner
    }

    /// Resolves once the fd is readable (or closed / in error).
    pub fn readable(&self) -> Readiness<'_> {
        Readiness {
            state: &self.state,
            write: false,
            key: None,
        }
    }

    /// Resolves once the fd is writable (or closed / in error).
    pub fn writable(&self) -> Readiness<'_> {
        Readiness {
            state: &self.state,
            write: true,
            key: None,
        }
    }

    /// Forget cached read readiness, after a read returned `WouldBlock`.
    pub fn clear_read_ready(&self) {
        self.state.ready.set(self.state.ready.get() & !READ_READY);
    }

    /// Forget cached write readiness, after a write returned `WouldBlock`.
    pub fn clear_write_ready(&self) {
        self.state.ready.set(self.state.ready.get() & !WRITE_READY);
    }

    /// Run `op` until it does not return `WouldBlock`, waiting for read
    /// readiness in between.
    pub async fn read_with<R>(&self, mut op: impl FnMut(&T) -> io::Result<R>) -> io::Result<R> {
        loop {
            match op(&self.inner) {
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    self.clear_read_ready();
                    self.readable().await;
                }
                res => return res,
            }
        }
    }

    /// Run `op` until it does not return `WouldBlock`, waiting for write
    /// readiness in between.
    pub async fn write_with<R>(&self, mut op: impl FnMut(&T) -> io::Result<R>) -> io::Result<R> {
        loop {
            match op(&self.inner) {
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    self.clear_write_ready();
                    self.writable().await;
                }
                res => return res,
            }
        }
    }

    /// Send all of `data` on a socket, waiting for room as needed. A peer
    /// that went away is an error, not a `SIGPIPE`.
    pub async fn send_all(&self, mut data: &[u8]) -> io::Result<()> {
        while !data.is_empty() {
            let sent = self
                .write_with(|socket| os::send(socket.as_fd(), data))
                .await?;
            data = &data[sent..];
        }
        Ok(())
    }

    /// The underlying event source, e.g. to set its priority.
    pub fn source(&self) -> &EventSource {
        &self.src
    }
}

/// Future returned by [`AsyncFd::readable`] and [`AsyncFd::writable`].
pub struct Readiness<'a> {
    state: &'a IoState,
    write: bool,
    key: Option<u64>,
}

impl Future for Readiness<'_> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        let waiters = this.state.waiters(this.write);
        if this.state.is_ready(this.write) {
            waiters.deregister(this.key.take());
            Poll::Ready(())
        } else {
            waiters.register(&mut this.key, waker::of(cx));
            Poll::Pending
        }
    }
}

impl Drop for Readiness<'_> {
    fn drop(&mut self) {
        self.state.waiters(self.write).deregister(self.key);
    }
}
