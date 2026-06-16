//! Inotify watches.

use std::ffi::CStr;
use std::io;
use std::os::raw::c_int;
use std::os::unix::io::RawFd;
use std::rc::Rc;

use super::current_event;
use super::slots::{Queue, Recv};
use crate::{EventSource, InotifyEvent};

/// Owned snapshot of an `inotify_event`.
#[derive(Debug, Clone)]
pub struct InotifyEventOwned {
    pub wd: c_int,
    pub mask: u32,
    pub cookie: u32,
    pub name: Option<std::ffi::CString>,
}

impl InotifyEventOwned {
    fn from_borrowed(ev: &InotifyEvent) -> Self {
        Self {
            wd: ev.watch_descriptor(),
            mask: ev.mask(),
            cookie: ev.cookie(),
            name: ev.name().map(|c| c.to_owned()),
        }
    }
}

/// An inotify watch whose events queue up until consumed.
pub struct Inotify {
    queue: Rc<Queue<InotifyEventOwned>>,
    _src: EventSource,
}

/// Watch `path` for events in `mask` (see `inotify(7)`).
pub fn inotify(path: &CStr, mask: u32) -> io::Result<Inotify> {
    let queue: Rc<Queue<InotifyEventOwned>> = Rc::default();
    let q = queue.clone();
    let src = current_event()?.add_inotify(path, mask, move |ev| {
        q.push(InotifyEventOwned::from_borrowed(ev));
        0
    })?;
    Ok(Inotify { queue, _src: src })
}

/// Like [`inotify`] but takes an already-open fd (typically `O_PATH`).
pub fn inotify_fd(fd: RawFd, mask: u32) -> io::Result<Inotify> {
    let queue: Rc<Queue<InotifyEventOwned>> = Rc::default();
    let q = queue.clone();
    let src = current_event()?.add_inotify_fd(fd, mask, move |ev| {
        q.push(InotifyEventOwned::from_borrowed(ev));
        0
    })?;
    Ok(Inotify { queue, _src: src })
}

impl Inotify {
    /// Wait for the next event (or take one that already arrived).
    pub fn recv(&mut self) -> Recv<'_, InotifyEventOwned> {
        self.queue.recv()
    }
}
