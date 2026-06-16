//! Event sources: the handles `Event::add_*` return.
#![expect(
    clippy::undocumented_unsafe_blocks,
    reason = "every block passes sd-event the pointer of the EventSource, which holds a reference to it"
)]

use std::ffi::{CStr, CString};
use std::io;
use std::marker::PhantomData;
use std::os::raw::{c_int, c_uint};
use std::os::unix::io::RawFd;
use std::ptr::{self, NonNull};

use libc::{clockid_t, pid_t, siginfo_t};

use super::callbacks::*;
use super::{Enabled, Event, MemoryPressureType, check};
use crate::sys;

/// An event source attached to an [`Event`] loop.
///
/// Dropping the handle releases one reference. The source can still fire
/// while a reference is held — use [`EventSource::set_enabled`]
/// ([`Enabled::Off`]) or [`EventSource::disable_unref`] to *stop* dispatch.
pub struct EventSource {
    pub(super) inner: NonNull<sys::sd_event_source>,
    pub(super) _not_send: PhantomData<*mut ()>,
}

impl EventSource {
    /// Raw pointer to the underlying `sd_event_source`.
    pub fn as_ptr(&self) -> *mut sys::sd_event_source {
        self.inner.as_ptr()
    }

    /// Borrow the event loop this source belongs to. The returned [`Event`]
    /// holds its own reference (cloned via `sd_event_source_get_event(3)` +
    /// `sd_event_ref(3)`).
    pub fn event(&self) -> Event {
        let raw = unsafe { sys::sd_event_source_get_event(self.as_ptr()) };
        unsafe { sys::sd_event_ref(raw) };
        Event::from_raw(raw)
    }

    // ----- Generic source ops -----

    /// Attach a descriptive name (used in libsystemd debug logging).
    ///
    /// Wraps `sd_event_source_set_description(3)`.
    pub fn set_description(&self, desc: &str) -> io::Result<()> {
        let c = CString::new(desc).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        check(unsafe { sys::sd_event_source_set_description(self.as_ptr(), c.as_ptr()) })
            .map(|_| ())
    }

    /// Currently-attached description, if any.
    ///
    /// Wraps `sd_event_source_get_description(3)`. Returns `Ok(None)` when
    /// the source has no description set.
    pub fn description(&self) -> io::Result<Option<String>> {
        let mut p: *const std::os::raw::c_char = ptr::null();
        let r = unsafe { sys::sd_event_source_get_description(self.as_ptr(), &mut p) };
        if r == -libc::ENXIO {
            return Ok(None);
        }
        check(r)?;
        if p.is_null() {
            return Ok(None);
        }
        Ok(Some(
            unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned(),
        ))
    }

    /// Set the dispatch priority. Lower priorities fire first.
    ///
    /// See [`crate::priority::IMPORTANT`], [`crate::priority::NORMAL`], [`crate::priority::IDLE`].
    /// Wraps `sd_event_source_set_priority(3)`.
    pub fn set_priority(&self, priority: i64) -> io::Result<()> {
        check(unsafe { sys::sd_event_source_set_priority(self.as_ptr(), priority) }).map(|_| ())
    }

    /// Current dispatch priority.
    ///
    /// Wraps `sd_event_source_get_priority(3)`.
    pub fn priority(&self) -> io::Result<i64> {
        let mut out: i64 = 0;
        check(unsafe { sys::sd_event_source_get_priority(self.as_ptr(), &mut out) })?;
        Ok(out)
    }

    /// Enable or disable the source.
    ///
    /// Wraps `sd_event_source_set_enabled(3)`.
    pub fn set_enabled(&self, enabled: Enabled) -> io::Result<()> {
        check(unsafe { sys::sd_event_source_set_enabled(self.as_ptr(), enabled as c_int) })
            .map(|_| ())
    }

    /// Current enable state. Wraps `sd_event_source_get_enabled(3)`.
    pub fn enabled(&self) -> io::Result<Enabled> {
        let mut out: c_int = 0;
        check(unsafe { sys::sd_event_source_get_enabled(self.as_ptr(), &mut out) })?;
        Ok(Enabled::from_raw(out))
    }

    /// Whether the source currently has unhandled pending events.
    ///
    /// Wraps `sd_event_source_get_pending(3)`. Not supported on exit
    /// sources (raw errno `EDOM`).
    pub fn is_pending(&self) -> io::Result<bool> {
        Ok(check(unsafe { sys::sd_event_source_get_pending(self.as_ptr()) })? > 0)
    }

    /// Install a prepare callback. Returning a negative value from it
    /// disables the source (but doesn't exit the loop).
    ///
    /// Wraps `sd_event_source_set_prepare(3)`. Not supported on exit
    /// sources. Fails with `EBUSY` when called from inside the prepare
    /// callback it would replace.
    pub fn set_prepare<F>(&self, callback: F) -> io::Result<()>
    where
        F: FnMut() -> i32 + 'static,
    {
        replace_callback(&self.state()?.prepare, Some(Box::new(callback)))?;
        check(unsafe { sys::sd_event_source_set_prepare(self.as_ptr(), Some(prepare_trampoline)) })
            .map(|_| ())
    }

    /// Detach any prepare callback. Wraps `sd_event_source_set_prepare(3)`
    /// with a NULL handler.
    ///
    /// Fails with `EBUSY` when called from inside the prepare callback.
    pub fn clear_prepare(&self) -> io::Result<()> {
        if let Ok(state) = self.state() {
            replace_callback(&state.prepare, None)?;
        }
        check(unsafe { sys::sd_event_source_set_prepare(self.as_ptr(), None) }).map(|_| ())
    }

    /// Convert this source into a "floating" source owned by the loop.
    ///
    /// After this call the loop holds the source's reference; the handle is
    /// consumed because operating on it further would be unsound. The
    /// source remains alive until the loop is freed. Wraps
    /// `sd_event_source_set_floating(3)`.
    ///
    /// **Caveat**: a source created non-floating retains its internal
    /// ref-to-the-loop even after `into_floating`, which means dropping
    /// your last [`Event`] handle does not by itself tear down the loop —
    /// the floating source keeps it alive. To shut things down, drive the
    /// loop to completion (e.g. via [`Event::set_exit_on_idle`] +
    /// [`Event::run`]) before dropping the [`Event`].
    pub fn into_floating(self) -> io::Result<()> {
        check(unsafe { sys::sd_event_source_set_floating(self.as_ptr(), 1) })?;
        std::mem::forget(self);
        Ok(())
    }

    /// Whether this source is currently floating (owned by the loop).
    ///
    /// Wraps `sd_event_source_get_floating(3)`.
    pub fn is_floating(&self) -> io::Result<bool> {
        Ok(check(unsafe { sys::sd_event_source_get_floating(self.as_ptr()) })? > 0)
    }

    /// When true, a negative-returning handler exits the entire loop with
    /// that negative value as the exit code. When false (default), only the
    /// source itself is disabled. Wraps
    /// `sd_event_source_set_exit_on_failure(3)`.
    pub fn set_exit_on_failure(&self, b: bool) -> io::Result<()> {
        check(unsafe { sys::sd_event_source_set_exit_on_failure(self.as_ptr(), b as c_int) })
            .map(|_| ())
    }

    /// Wraps `sd_event_source_get_exit_on_failure(3)`.
    pub fn exit_on_failure(&self) -> io::Result<bool> {
        Ok(check(unsafe { sys::sd_event_source_get_exit_on_failure(self.as_ptr()) })? > 0)
    }

    /// Disable the source and drop the local reference in one step.
    ///
    /// Use this when you must guarantee the source cannot fire one last
    /// time before being freed. Wraps `sd_event_source_disable_unref(3)`.
    pub fn disable_unref(self) {
        unsafe { sys::sd_event_source_disable_unref(self.as_ptr()) };
        std::mem::forget(self);
    }

    // ----- Rate limiting -----

    /// Temporarily disable the source if it dispatches more than `burst`
    /// times in `interval_us`. Passing 0 for either disables rate
    /// limiting. Wraps `sd_event_source_set_ratelimit(3)`.
    pub fn set_ratelimit(&self, interval_us: u64, burst: u32) -> io::Result<()> {
        check(unsafe {
            sys::sd_event_source_set_ratelimit(self.as_ptr(), interval_us, burst as c_uint)
        })
        .map(|_| ())
    }

    /// Configured rate-limit `(interval_us, burst)`, or `Ok(None)` if none
    /// is set (raw errno `ENOEXEC`).
    ///
    /// Wraps `sd_event_source_get_ratelimit(3)`.
    pub fn ratelimit(&self) -> io::Result<Option<(u64, u32)>> {
        let mut iv: u64 = 0;
        let mut burst: c_uint = 0;
        let r = unsafe { sys::sd_event_source_get_ratelimit(self.as_ptr(), &mut iv, &mut burst) };
        if r == -libc::ENOEXEC {
            return Ok(None);
        }
        check(r)?;
        Ok(Some((iv, burst as u32)))
    }

    /// Whether the source is currently in the temporarily-disabled
    /// rate-limited state. Wraps `sd_event_source_is_ratelimited(3)`.
    pub fn is_ratelimited(&self) -> io::Result<bool> {
        Ok(check(unsafe { sys::sd_event_source_is_ratelimited(self.as_ptr()) })? > 0)
    }

    /// Immediately leave the rate-limited state and reset counters.
    ///
    /// Returns `true` if the source had been rate-limited. Wraps
    /// `sd_event_source_leave_ratelimit(3)`.
    pub fn leave_ratelimit(&self) -> io::Result<bool> {
        Ok(check(unsafe { sys::sd_event_source_leave_ratelimit(self.as_ptr()) })? > 0)
    }

    /// Install a callback fired whenever the source transitions out of the
    /// rate-limited state.
    ///
    /// Wraps `sd_event_source_set_ratelimit_expire_callback(3)`.
    pub fn set_ratelimit_expire_callback<F>(&self, callback: F) -> io::Result<()>
    where
        F: FnMut() -> i32 + 'static,
    {
        replace_callback(&self.state()?.ratelimit_expire, Some(Box::new(callback)))?;
        check(unsafe {
            sys::sd_event_source_set_ratelimit_expire_callback(
                self.as_ptr(),
                Some(ratelimit_expire_trampoline),
            )
        })
        .map(|_| ())
    }

    // ----- I/O source ops -----

    /// Currently-watched fd. Wraps `sd_event_source_get_io_fd(3)`.
    pub fn io_fd(&self) -> io::Result<RawFd> {
        check(unsafe { sys::sd_event_source_get_io_fd(self.as_ptr()) })
    }

    /// Swap in a different fd. If the previous fd was owned by the source
    /// it is closed. Wraps `sd_event_source_set_io_fd(3)`.
    pub fn set_io_fd(&self, fd: RawFd) -> io::Result<()> {
        check(unsafe { sys::sd_event_source_set_io_fd(self.as_ptr(), fd) }).map(|_| ())
    }

    /// Whether the source owns (i.e. closes on destroy) its fd.
    ///
    /// Wraps `sd_event_source_get_io_fd_own(3)`.
    pub fn io_fd_own(&self) -> io::Result<bool> {
        Ok(check(unsafe { sys::sd_event_source_get_io_fd_own(self.as_ptr()) })? > 0)
    }

    /// Hand fd ownership to the source (or take it back).
    ///
    /// Wraps `sd_event_source_set_io_fd_own(3)`.
    pub fn set_io_fd_own(&self, own: bool) -> io::Result<()> {
        check(unsafe { sys::sd_event_source_set_io_fd_own(self.as_ptr(), own as c_int) })
            .map(|_| ())
    }

    /// Currently-configured epoll event mask.
    ///
    /// Wraps `sd_event_source_get_io_events(3)`.
    pub fn io_events(&self) -> io::Result<u32> {
        let mut out: u32 = 0;
        check(unsafe { sys::sd_event_source_get_io_events(self.as_ptr(), &mut out) })?;
        Ok(out)
    }

    /// Replace the epoll event mask. `EPOLLHUP`/`EPOLLERR` are always
    /// reported regardless. Wraps `sd_event_source_set_io_events(3)`.
    pub fn set_io_events(&self, events: u32) -> io::Result<()> {
        check(unsafe { sys::sd_event_source_set_io_events(self.as_ptr(), events) }).map(|_| ())
    }

    /// Last seen but undispatched epoll event mask.
    ///
    /// Wraps `sd_event_source_get_io_revents(3)`.
    pub fn io_revents(&self) -> io::Result<u32> {
        let mut out: u32 = 0;
        check(unsafe { sys::sd_event_source_get_io_revents(self.as_ptr(), &mut out) })?;
        Ok(out)
    }

    // ----- Time source ops -----

    /// Currently-configured trigger time (absolute μs since the clock
    /// epoch). Wraps `sd_event_source_get_time(3)`.
    pub fn time(&self) -> io::Result<u64> {
        let mut out: u64 = 0;
        check(unsafe { sys::sd_event_source_get_time(self.as_ptr(), &mut out) })?;
        Ok(out)
    }

    /// Update the absolute trigger time. Wraps
    /// `sd_event_source_set_time(3)`.
    pub fn set_time(&self, usec: u64) -> io::Result<()> {
        check(unsafe { sys::sd_event_source_set_time(self.as_ptr(), usec) }).map(|_| ())
    }

    /// Update the trigger time relative to the current iteration's clock
    /// reading. Wraps `sd_event_source_set_time_relative(3)`.
    pub fn set_time_relative(&self, usec: u64) -> io::Result<()> {
        check(unsafe { sys::sd_event_source_set_time_relative(self.as_ptr(), usec) }).map(|_| ())
    }

    /// Currently-configured accuracy (μs). 0 means the 250 ms default.
    ///
    /// Wraps `sd_event_source_get_time_accuracy(3)`.
    pub fn time_accuracy(&self) -> io::Result<u64> {
        let mut out: u64 = 0;
        check(unsafe { sys::sd_event_source_get_time_accuracy(self.as_ptr(), &mut out) })?;
        Ok(out)
    }

    /// Update accuracy (μs). Wraps `sd_event_source_set_time_accuracy(3)`.
    pub fn set_time_accuracy(&self, usec: u64) -> io::Result<()> {
        check(unsafe { sys::sd_event_source_set_time_accuracy(self.as_ptr(), usec) }).map(|_| ())
    }

    /// Clock id the timer is using.
    ///
    /// Wraps `sd_event_source_get_time_clock(3)`.
    pub fn time_clock(&self) -> io::Result<clockid_t> {
        let mut out: clockid_t = 0;
        check(unsafe { sys::sd_event_source_get_time_clock(self.as_ptr(), &mut out) })?;
        Ok(out)
    }

    // ----- Signal source ops -----

    /// Signal number watched by this signal source.
    ///
    /// Wraps `sd_event_source_get_signal(3)`.
    pub fn signal(&self) -> io::Result<c_int> {
        check(unsafe { sys::sd_event_source_get_signal(self.as_ptr()) })
    }

    // ----- Child source ops -----

    /// PID of the watched child.
    ///
    /// Wraps `sd_event_source_get_child_pid(3)`.
    pub fn child_pid(&self) -> io::Result<pid_t> {
        let mut out: pid_t = 0;
        check(unsafe { sys::sd_event_source_get_child_pid(self.as_ptr(), &mut out) })?;
        Ok(out)
    }

    /// Pidfd referencing the watched child.
    ///
    /// Wraps `sd_event_source_get_child_pidfd(3)`.
    pub fn child_pidfd(&self) -> io::Result<RawFd> {
        check(unsafe { sys::sd_event_source_get_child_pidfd(self.as_ptr()) })
    }

    /// Whether the source owns (closes on destroy) the pidfd.
    ///
    /// Wraps `sd_event_source_get_child_pidfd_own(3)`.
    pub fn child_pidfd_own(&self) -> io::Result<bool> {
        Ok(check(unsafe { sys::sd_event_source_get_child_pidfd_own(self.as_ptr()) })? > 0)
    }

    /// Wraps `sd_event_source_set_child_pidfd_own(3)`.
    pub fn set_child_pidfd_own(&self, own: bool) -> io::Result<()> {
        check(unsafe { sys::sd_event_source_set_child_pidfd_own(self.as_ptr(), own as c_int) })
            .map(|_| ())
    }

    /// Whether the source will `SIGKILL`+reap the watched process when
    /// freed. Wraps `sd_event_source_get_child_process_own(3)`.
    pub fn child_process_own(&self) -> io::Result<bool> {
        Ok(check(unsafe { sys::sd_event_source_get_child_process_own(self.as_ptr()) })? > 0)
    }

    /// Wraps `sd_event_source_set_child_process_own(3)`.
    pub fn set_child_process_own(&self, own: bool) -> io::Result<()> {
        check(unsafe { sys::sd_event_source_set_child_process_own(self.as_ptr(), own as c_int) })
            .map(|_| ())
    }

    /// Send a signal to the watched child via `pidfd_send_signal(2)`.
    ///
    /// `flags` must be 0 at present. Wraps
    /// `sd_event_source_send_child_signal(3)`.
    pub fn send_child_signal(
        &self,
        sig: c_int,
        info: Option<&siginfo_t>,
        flags: u32,
    ) -> io::Result<()> {
        let info_ptr = info.map(|i| i as *const siginfo_t).unwrap_or(ptr::null());
        check(unsafe {
            sys::sd_event_source_send_child_signal(self.as_ptr(), sig, info_ptr, flags as c_uint)
        })
        .map(|_| ())
    }

    // ----- Inotify source ops -----

    /// Configured inotify watch mask.
    ///
    /// Wraps `sd_event_source_get_inotify_mask(3)`.
    pub fn inotify_mask(&self) -> io::Result<u32> {
        let mut out: u32 = 0;
        check(unsafe { sys::sd_event_source_get_inotify_mask(self.as_ptr(), &mut out) })?;
        Ok(out)
    }

    /// Watched path (`-ESTALE` once the inode is no longer valid).
    ///
    /// Wraps `sd_event_source_get_inotify_path(3)` (available since v256).
    pub fn inotify_path(&self) -> io::Result<String> {
        let mut p: *const std::os::raw::c_char = ptr::null();
        check(unsafe { sys::sd_event_source_get_inotify_path(self.as_ptr(), &mut p) })?;
        if p.is_null() {
            return Err(io::Error::other("null inotify path"));
        }
        Ok(unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned())
    }

    // ----- Memory pressure source ops -----

    /// Choose the PSI type (`"some"` or `"full"`) for a memory-pressure
    /// source. Must be called immediately after creation, before the loop
    /// starts polling. Wraps `sd_event_source_set_memory_pressure_type(3)`.
    pub fn set_memory_pressure_type(&self, ty: MemoryPressureType) -> io::Result<()> {
        check(unsafe {
            sys::sd_event_source_set_memory_pressure_type(self.as_ptr(), ty.as_cstr().as_ptr())
        })
        .map(|_| ())
    }

    /// Set the PSI threshold and window in microseconds. Wraps
    /// `sd_event_source_set_memory_pressure_period(3)`.
    pub fn set_memory_pressure_period(&self, threshold_us: u64, window_us: u64) -> io::Result<()> {
        check(unsafe {
            sys::sd_event_source_set_memory_pressure_period(self.as_ptr(), threshold_us, window_us)
        })
        .map(|_| ())
    }

    // ----- Internal -----

    fn state(&self) -> io::Result<&SourceState> {
        let ud = unsafe { sys::sd_event_source_get_userdata(self.as_ptr()) };
        if ud.is_null() {
            return Err(io::Error::other("event source has no Rust-owned state"));
        }
        // The state lives until the destroy callback, which cannot run
        // while `self` holds a reference to the source.
        Ok(unsafe { &*(ud as *const SourceState) })
    }
}

impl Clone for EventSource {
    fn clone(&self) -> Self {
        unsafe { sys::sd_event_source_ref(self.as_ptr()) };
        Self {
            inner: self.inner,
            _not_send: PhantomData,
        }
    }
}

impl Drop for EventSource {
    fn drop(&mut self) {
        unsafe {
            sys::sd_event_source_unref(self.as_ptr());
        }
    }
}

// =============================================================================
// Inotify event helper
// =============================================================================

/// A pointer-aligned view of an `inotify_event`. The name field is
/// variable-length; access it via [`InotifyEvent::name`].
#[repr(transparent)]
pub struct InotifyEvent(libc::inotify_event);

impl InotifyEvent {
    /// Inotify watch descriptor.
    pub fn watch_descriptor(&self) -> c_int {
        self.0.wd
    }
    /// Bit mask of `IN_*` events that occurred.
    pub fn mask(&self) -> u32 {
        self.0.mask
    }
    /// Unique cookie associating rename pairs.
    pub fn cookie(&self) -> u32 {
        self.0.cookie
    }
    /// Length (including NUL padding) of the name field, possibly 0.
    pub fn name_len(&self) -> u32 {
        self.0.len
    }

    /// File name component if present (`len > 0`).
    ///
    /// The returned `&CStr` borrows from the underlying inotify buffer
    /// owned by libsystemd; treat it as live only for the duration of the
    /// inotify callback.
    pub fn name(&self) -> Option<&CStr> {
        if self.0.len == 0 {
            return None;
        }
        unsafe {
            let base = self as *const _ as *const u8;
            let name_ptr = base.add(std::mem::size_of::<libc::inotify_event>());
            Some(CStr::from_ptr(name_ptr as *const std::os::raw::c_char))
        }
    }
}
