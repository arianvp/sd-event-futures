//! The Rust closures behind event sources, and the C trampolines that call
//! them.

use std::cell::RefCell;
use std::io;
use std::marker::PhantomData;
use std::os::raw::{c_int, c_void};
use std::os::unix::io::RawFd;
use std::ptr::NonNull;

use libc::{siginfo_t, signalfd_siginfo};

use super::{EventSource, InotifyEvent};
use crate::sys;

pub(super) type PlainCb = Box<dyn FnMut() -> i32>;
pub(super) type IoCb = Box<dyn FnMut(RawFd, u32) -> i32>;
pub(super) type TimeCb = Box<dyn FnMut(u64) -> i32>;
pub(super) type SignalCb = Box<dyn FnMut(&signalfd_siginfo) -> i32>;
pub(super) type ChildCb = Box<dyn FnMut(&siginfo_t) -> i32>;
pub(super) type InotifyCb = Box<dyn FnMut(&InotifyEvent) -> i32>;

/// Every callback sits in its own `RefCell` and trampolines only take a
/// shared reference to the state, so a callback may install or clear a
/// *different* callback on its own source. Replacing the callback that is
/// currently running fails with `EBUSY` instead of freeing it mid-call.
#[derive(Default)]
pub(super) struct SourceState {
    pub(super) handler_plain: RefCell<Option<PlainCb>>,
    pub(super) handler_io: RefCell<Option<IoCb>>,
    pub(super) handler_time: RefCell<Option<TimeCb>>,
    pub(super) handler_signal: RefCell<Option<SignalCb>>,
    pub(super) handler_child: RefCell<Option<ChildCb>>,
    pub(super) handler_inotify: RefCell<Option<InotifyCb>>,
    pub(super) prepare: RefCell<Option<PlainCb>>,
    pub(super) ratelimit_expire: RefCell<Option<PlainCb>>,
}

pub(super) fn replace_callback<C>(slot: &RefCell<Option<C>>, cb: Option<C>) -> io::Result<()> {
    let old = {
        let mut slot = slot
            .try_borrow_mut()
            .map_err(|_| io::Error::from_raw_os_error(libc::EBUSY))?;
        std::mem::replace(&mut *slot, cb)
    };
    drop(old);
    Ok(())
}

/// Run a user closure under `catch_unwind`. A panic becomes `-EIO`,
/// which is a negative return from sd-event's perspective — the default
/// behavior is to disable the offending source (see
/// `sd_event_source_set_exit_on_failure(3)`).
pub(super) fn guard<F: FnOnce() -> i32>(f: F) -> c_int {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(n) => n,
        Err(_) => -libc::EIO,
    }
}

pub(super) fn dispatch<C>(slot: &RefCell<Option<C>>, call: impl FnOnce(&mut C) -> i32) -> c_int {
    let Ok(mut cb) = slot.try_borrow_mut() else {
        return -libc::EBUSY;
    };
    let Some(cb) = cb.as_mut() else {
        return 0;
    };
    guard(|| call(cb))
}

/// The state behind a source's `userdata`.
///
/// # Safety
///
/// `userdata` must be what a source was created with: a `SourceState` from
/// `Box::into_raw()`, which lives until `state_destroy()` frees it.
pub(super) unsafe fn state<'a>(userdata: *mut c_void) -> &'a SourceState {
    // SAFETY: as the caller promises.
    unsafe { &*(userdata as *const SourceState) }
}

// The trampolines below are only ever called by sd-event, with the
// `userdata` the source was created with and event data that is valid for
// the duration of the call.

pub(super) unsafe extern "C" fn plain_trampoline(
    _s: *mut sys::sd_event_source,
    userdata: *mut c_void,
) -> c_int {
    // SAFETY: see above.
    let state = unsafe { state(userdata) };
    dispatch(&state.handler_plain, |h| h())
}

pub(super) unsafe extern "C" fn io_trampoline(
    _s: *mut sys::sd_event_source,
    fd: c_int,
    revents: u32,
    userdata: *mut c_void,
) -> c_int {
    // SAFETY: see above.
    let state = unsafe { state(userdata) };
    dispatch(&state.handler_io, |h| h(fd, revents))
}

pub(super) unsafe extern "C" fn time_trampoline(
    _s: *mut sys::sd_event_source,
    usec: u64,
    userdata: *mut c_void,
) -> c_int {
    // SAFETY: see above.
    let state = unsafe { state(userdata) };
    dispatch(&state.handler_time, |h| h(usec))
}

pub(super) unsafe extern "C" fn signal_trampoline(
    _s: *mut sys::sd_event_source,
    si: *const signalfd_siginfo,
    userdata: *mut c_void,
) -> c_int {
    // SAFETY: see above.
    let (state, info) = unsafe { (state(userdata), &*si) };
    dispatch(&state.handler_signal, |h| h(info))
}

pub(super) unsafe extern "C" fn child_trampoline(
    _s: *mut sys::sd_event_source,
    si: *const siginfo_t,
    userdata: *mut c_void,
) -> c_int {
    // SAFETY: see above.
    let (state, info) = unsafe { (state(userdata), &*si) };
    dispatch(&state.handler_child, |h| h(info))
}

pub(super) unsafe extern "C" fn inotify_trampoline(
    _s: *mut sys::sd_event_source,
    ev: *const libc::inotify_event,
    userdata: *mut c_void,
) -> c_int {
    // SAFETY: see above; `InotifyEvent` is a transparent wrapper.
    let (state, ev) = unsafe { (state(userdata), &*(ev as *const InotifyEvent)) };
    dispatch(&state.handler_inotify, |h| h(ev))
}

pub(super) unsafe extern "C" fn prepare_trampoline(
    _s: *mut sys::sd_event_source,
    userdata: *mut c_void,
) -> c_int {
    // SAFETY: see above.
    let state = unsafe { state(userdata) };
    dispatch(&state.prepare, |h| h())
}

pub(super) unsafe extern "C" fn ratelimit_expire_trampoline(
    _s: *mut sys::sd_event_source,
    userdata: *mut c_void,
) -> c_int {
    // SAFETY: see above.
    let state = unsafe { state(userdata) };
    dispatch(&state.ratelimit_expire, |h| h())
}

pub(super) unsafe extern "C" fn state_destroy(userdata: *mut c_void) {
    // SAFETY: sd-event calls this once, as the source is freed, with the
    // `Box::into_raw()` pointer the source was created with.
    drop(unsafe { Box::from_raw(userdata as *mut SourceState) });
}

/// Wrap the result of an `sd_event_add_*()` call made with `userdata`, a
/// `SourceState` from `Box::into_raw()`: on success hand the state to the
/// source, which frees it when it goes, on failure free it here.
pub(super) fn finish_source(
    r: c_int,
    src: *mut sys::sd_event_source,
    userdata: *mut SourceState,
) -> io::Result<EventSource> {
    // SAFETY: on failure no source exists that could have taken the state.
    let free_state = || drop(unsafe { Box::from_raw(userdata) });
    if r < 0 {
        free_state();
        return Err(io::Error::from_raw_os_error(-r));
    }
    // SAFETY: `src` is the source the successful call created.
    let dr = unsafe { sys::sd_event_source_set_destroy_callback(src, Some(state_destroy)) };
    if dr < 0 {
        // SAFETY: drops the only reference; without a destroy callback the
        // state is still ours to free.
        unsafe { sys::sd_event_source_unref(src) };
        free_state();
        return Err(io::Error::from_raw_os_error(-dr));
    }
    Ok(EventSource {
        inner: NonNull::new(src).expect("sd_event_add_* returned null on success"),
        _not_send: PhantomData,
    })
}
