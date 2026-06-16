//! Safe Rust wrapper around libsystemd's [`sd-event`] event loop.
//!
//! All doc comments paraphrase the corresponding `sd_event(3)` /
//! `sd_event_*(3)` man pages. The wrapper covers the full surface of
//! `sd-event.h`: I/O, timer, signal, child, inotify, defer, post, exit,
//! and memory-pressure sources, plus every per-source operation
//! (priorities, prepare callbacks, ratelimits, exit-on-failure, …).
//!
//! Every `unsafe` block here calls into `sd-event` with a pointer that the
//! surrounding [`Event`] or [`EventSource`] owns a reference to, which keeps
//! it alive for the call.
//!
//! [`sd-event`]: https://man.archlinux.org/man/sd-event.3.en
#![expect(
    clippy::undocumented_unsafe_blocks,
    reason = "every block passes sd-event a pointer that the surrounding Event or EventSource holds a reference to"
)]

mod callbacks;
mod source;

use std::cell::RefCell;
use std::ffi::CStr;
use std::io;
use std::marker::PhantomData;
use std::os::raw::{c_int, c_void};
use std::os::unix::io::RawFd;
use std::ptr::{self, NonNull};

use libc::{clockid_t, pid_t, siginfo_t, signalfd_siginfo};

use crate::sys;
use callbacks::*;
pub use source::{EventSource, InotifyEvent};

/// `clockid_t` constants accepted by [`Event::add_time`] and friends.
///
/// `_ALARM` clocks (only available to a privileged process) can wake the
/// machine from suspend. See `sd_event_add_time(3)`.
pub mod clock {
    use libc::clockid_t;
    pub const REALTIME: clockid_t = libc::CLOCK_REALTIME;
    pub const MONOTONIC: clockid_t = libc::CLOCK_MONOTONIC;
    pub const BOOTTIME: clockid_t = libc::CLOCK_BOOTTIME;
    pub const REALTIME_ALARM: clockid_t = libc::CLOCK_REALTIME_ALARM;
    pub const BOOTTIME_ALARM: clockid_t = libc::CLOCK_BOOTTIME_ALARM;
}

/// Re-exported priority reference points. Lower priorities dispatch first.
///
/// See `sd_event_source_set_priority(3)`.
pub mod priority {
    pub const IMPORTANT: i64 = super::sys::SD_EVENT_PRIORITY_IMPORTANT;
    pub const NORMAL: i64 = super::sys::SD_EVENT_PRIORITY_NORMAL;
    pub const IDLE: i64 = super::sys::SD_EVENT_PRIORITY_IDLE;
}

/// Bit that can be OR'd into a signal number passed to [`Event::add_signal`]
/// so libsystemd blocks the signal in the calling thread automatically.
///
/// See `sd_event_add_signal(3)`.
pub const SIGNAL_PROCMASK: c_int = sys::SD_EVENT_SIGNAL_PROCMASK;

/// Enable state of an event source. See `sd_event_source_set_enabled(3)`.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum Enabled {
    /// Source disabled — no dispatch and no wakeup.
    Off = sys::SD_EVENT_OFF,
    /// Source is dispatched continuously while pending.
    On = sys::SD_EVENT_ON,
    /// Source fires once then auto-resets to [`Enabled::Off`].
    OneShot = sys::SD_EVENT_ONESHOT,
}

impl Enabled {
    fn from_raw(v: c_int) -> Enabled {
        match v {
            sys::SD_EVENT_ON => Enabled::On,
            sys::SD_EVENT_ONESHOT => Enabled::OneShot,
            _ => Enabled::Off,
        }
    }
}

/// State of the event loop as reported by [`Event::state`].
///
/// See `sd_event_get_state(3)`.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum State {
    /// Loop is idle between iterations. Default initial state.
    Initial = sys::SD_EVENT_INITIAL,
    /// Inside [`Event::prepare`].
    Preparing = sys::SD_EVENT_PREPARING,
    /// After prepare, before [`Event::wait`].
    Armed = sys::SD_EVENT_ARMED,
    /// Events became pending; awaiting [`Event::dispatch`].
    Pending = sys::SD_EVENT_PENDING,
    /// A handler is currently running.
    Running = sys::SD_EVENT_RUNNING,
    /// Exit was requested; exit sources are dispatching.
    Exiting = sys::SD_EVENT_EXITING,
    /// Loop is done and should be released.
    Finished = sys::SD_EVENT_FINISHED,
}

impl State {
    fn from_raw(v: c_int) -> State {
        match v {
            sys::SD_EVENT_PREPARING => State::Preparing,
            sys::SD_EVENT_ARMED => State::Armed,
            sys::SD_EVENT_PENDING => State::Pending,
            sys::SD_EVENT_RUNNING => State::Running,
            sys::SD_EVENT_EXITING => State::Exiting,
            sys::SD_EVENT_FINISHED => State::Finished,
            _ => State::Initial,
        }
    }
}

/// `"some"` or `"full"` Pressure Stall Information type for a memory-pressure
/// source. See `sd_event_source_set_memory_pressure_type(3)`.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum MemoryPressureType {
    Some,
    Full,
}

impl MemoryPressureType {
    fn as_cstr(&self) -> &'static CStr {
        match self {
            MemoryPressureType::Some => c"some",
            MemoryPressureType::Full => c"full",
        }
    }
}

fn check(r: c_int) -> io::Result<c_int> {
    if r < 0 {
        Err(io::Error::from_raw_os_error(-r))
    } else {
        Ok(r)
    }
}

// =============================================================================
// Event loop
// =============================================================================

/// An sd-event main loop.
///
/// `Event` is reference-counted; cloning is via libsystemd's
/// `sd_event_ref(3)`. Loops are bound to the thread that runs them, so this
/// type is intentionally `!Send` / `!Sync`.
///
/// See `man 3 sd-event` for the big picture.
pub struct Event {
    inner: NonNull<sys::sd_event>,
    _not_send: PhantomData<*mut ()>,
}

impl Event {
    /// Allocate a fresh, unattached event loop.
    ///
    /// Wraps `sd_event_new(3)`. The returned loop holds one reference; drop
    /// the [`Event`] (or every clone) to release it.
    pub fn new() -> io::Result<Self> {
        let mut raw: *mut sys::sd_event = ptr::null_mut();
        check(unsafe { sys::sd_event_new(&mut raw) })?;
        Ok(Self::from_raw(raw))
    }

    /// Acquire (or lazily create) the calling thread's default loop.
    ///
    /// Wraps `sd_event_default(3)`. Subsequent calls from the same thread
    /// return the same object with an incremented reference count.
    pub fn thread_default() -> io::Result<Self> {
        let mut raw: *mut sys::sd_event = ptr::null_mut();
        check(unsafe { sys::sd_event_default(&mut raw) })?;
        Ok(Self::from_raw(raw))
    }

    fn from_raw(raw: *mut sys::sd_event) -> Self {
        Self {
            inner: NonNull::new(raw).expect("sd_event_* returned null on success"),
            _not_send: PhantomData,
        }
    }

    /// Raw pointer to the underlying `sd_event`. Use with care.
    pub fn as_ptr(&self) -> *mut sys::sd_event {
        self.inner.as_ptr()
    }

    /// Return the thread id this default loop is bound to.
    ///
    /// Wraps `sd_event_get_tid(3)`. Only meaningful for loops obtained from
    /// [`Event::thread_default`]; otherwise returns `ENXIO`.
    pub fn tid(&self) -> io::Result<pid_t> {
        let mut out: pid_t = 0;
        check(unsafe { sys::sd_event_get_tid(self.as_ptr(), &mut out) })?;
        Ok(out)
    }

    /// Run the loop continuously until [`Event::exit`] is called. Returns
    /// the exit code.
    ///
    /// Wraps `sd_event_loop(3)`.
    pub fn run(&self) -> io::Result<i32> {
        check(unsafe { sys::sd_event_loop(self.as_ptr()) })
    }

    /// Run a single iteration. `timeout_us = None` blocks indefinitely.
    ///
    /// Returns `true` if a source was dispatched, `false` on timeout. Wraps
    /// `sd_event_run(3)`.
    pub fn run_once(&self, timeout_us: Option<u64>) -> io::Result<bool> {
        let t = timeout_us.unwrap_or(u64::MAX);
        Ok(check(unsafe { sys::sd_event_run(self.as_ptr(), t) })? > 0)
    }

    /// Run the prepare phase: invoke prepare callbacks, arm timers/epoll.
    ///
    /// Returns `true` if events are already pending (call [`Event::dispatch`]
    /// next); `false` means call [`Event::wait`]. Wraps `sd_event_prepare(3)`.
    pub fn prepare(&self) -> io::Result<bool> {
        Ok(check(unsafe { sys::sd_event_prepare(self.as_ptr()) })? > 0)
    }

    /// Block up to `timeout_us` microseconds for events to become pending.
    ///
    /// Returns `true` if events arrived, `false` on timeout. Wraps
    /// `sd_event_wait(3)`.
    pub fn wait(&self, timeout_us: Option<u64>) -> io::Result<bool> {
        let t = timeout_us.unwrap_or(u64::MAX);
        Ok(check(unsafe { sys::sd_event_wait(self.as_ptr(), t) })? > 0)
    }

    /// Dispatch the highest-priority pending source.
    ///
    /// Returns `true` if a source ran and the loop returned to
    /// [`State::Initial`] (iterate again starting at [`Event::prepare`]);
    /// `false` if the loop is exiting and no more sources will run. Wraps
    /// `sd_event_dispatch(3)`.
    pub fn dispatch(&self) -> io::Result<bool> {
        Ok(check(unsafe { sys::sd_event_dispatch(self.as_ptr()) })? > 0)
    }

    /// Current loop state. Wraps `sd_event_get_state(3)`.
    pub fn state(&self) -> io::Result<State> {
        let r = check(unsafe { sys::sd_event_get_state(self.as_ptr()) })?;
        Ok(State::from_raw(r))
    }

    /// Monotonically-increasing iteration counter, bumped at every prepare.
    ///
    /// Wraps `sd_event_get_iteration(3)`.
    pub fn iteration(&self) -> io::Result<u64> {
        let mut out: u64 = 0;
        check(unsafe { sys::sd_event_get_iteration(self.as_ptr(), &mut out) })?;
        Ok(out)
    }

    /// Ask the loop to terminate with the given exit code.
    ///
    /// Wraps `sd_event_exit(3)`. After this call, only sources added with
    /// [`Event::add_exit`] are dispatched (in priority order) before the
    /// loop transitions to [`State::Finished`].
    pub fn exit(&self, code: i32) -> io::Result<()> {
        check(unsafe { sys::sd_event_exit(self.as_ptr(), code) }).map(|_| ())
    }

    /// Exit code previously passed to [`Event::exit`].
    ///
    /// `Ok(None)` is returned if no exit has been requested yet (raw errno
    /// `ENODATA`). Wraps `sd_event_get_exit_code(3)`.
    pub fn exit_code(&self) -> io::Result<Option<i32>> {
        let mut out: c_int = 0;
        let r = unsafe { sys::sd_event_get_exit_code(self.as_ptr(), &mut out) };
        if r == -libc::ENODATA {
            return Ok(None);
        }
        check(r)?;
        Ok(Some(out))
    }

    /// Loop's internal epoll fd, suitable for embedding in another loop.
    ///
    /// Poll for `EPOLLIN`; do not call `epoll_ctl` on it directly. Wraps
    /// `sd_event_get_fd(3)`.
    pub fn fd(&self) -> io::Result<RawFd> {
        check(unsafe { sys::sd_event_get_fd(self.as_ptr()) })
    }

    /// Wall-clock-or-iteration time the current iteration started.
    ///
    /// If called before the first iteration, the current time is returned
    /// instead. Wraps `sd_event_now(3)`.
    pub fn now(&self, clock: clockid_t) -> io::Result<u64> {
        let mut out: u64 = 0;
        check(unsafe { sys::sd_event_now(self.as_ptr(), clock, &mut out) })?;
        Ok(out)
    }

    /// Exit automatically when no enabled non-exit, non-post sources remain.
    ///
    /// Wraps `sd_event_set_exit_on_idle(3)`.
    pub fn set_exit_on_idle(&self, b: bool) -> io::Result<()> {
        check(unsafe { sys::sd_event_set_exit_on_idle(self.as_ptr(), b as c_int) }).map(|_| ())
    }

    /// Whether exit-on-idle is currently enabled.
    ///
    /// Wraps `sd_event_get_exit_on_idle(3)`.
    pub fn exit_on_idle(&self) -> io::Result<bool> {
        Ok(check(unsafe { sys::sd_event_get_exit_on_idle(self.as_ptr()) })? > 0)
    }

    /// Install built-in handlers for `SIGINT` / `SIGTERM` so the loop exits
    /// on either. Wraps `sd_event_set_signal_exit(3)`.
    pub fn set_signal_exit(&self, b: bool) -> io::Result<()> {
        check(unsafe { sys::sd_event_set_signal_exit(self.as_ptr(), b as c_int) }).map(|_| ())
    }

    /// Enable periodic watchdog notifications to the service manager.
    ///
    /// Has no effect unless `$WATCHDOG_USEC` is set (typically by systemd).
    /// Wraps `sd_event_set_watchdog(3)`.
    pub fn set_watchdog(&self, b: bool) -> io::Result<bool> {
        Ok(check(unsafe { sys::sd_event_set_watchdog(self.as_ptr(), b as c_int) })? > 0)
    }

    /// Whether the watchdog is active. Wraps `sd_event_get_watchdog(3)`.
    pub fn watchdog(&self) -> io::Result<bool> {
        Ok(check(unsafe { sys::sd_event_get_watchdog(self.as_ptr()) })? > 0)
    }

    // ----- Source constructors -----

    /// Add an I/O source for `fd` watching the given epoll event mask
    /// (`EPOLLIN`, `EPOLLOUT`, …).
    ///
    /// Default enable mode is [`Enabled::On`]. The fd is **not** owned by
    /// the source; see [`EventSource::set_io_fd_own`] to hand it over.
    /// Wraps `sd_event_add_io(3)`.
    pub fn add_io<F>(&self, fd: RawFd, events: u32, callback: F) -> io::Result<EventSource>
    where
        F: FnMut(RawFd, u32) -> i32 + 'static,
    {
        let state = Box::new(SourceState {
            handler_io: RefCell::new(Some(Box::new(callback))),
            ..Default::default()
        });
        let userdata = Box::into_raw(state);
        let mut src: *mut sys::sd_event_source = ptr::null_mut();
        let r = unsafe {
            sys::sd_event_add_io(
                self.as_ptr(),
                &mut src,
                fd,
                events,
                Some(io_trampoline),
                userdata as *mut c_void,
            )
        };
        finish_source(r, src, userdata)
    }

    /// Add an absolute-time timer using `clock`. `usec` is microseconds
    /// since the clock's epoch; `accuracy_us = 0` selects the 250 ms
    /// default. Default enable mode is [`Enabled::OneShot`].
    ///
    /// Wraps `sd_event_add_time(3)`.
    pub fn add_time<F>(
        &self,
        clock: clockid_t,
        usec: u64,
        accuracy_us: u64,
        callback: F,
    ) -> io::Result<EventSource>
    where
        F: FnMut(u64) -> i32 + 'static,
    {
        self.add_time_impl(clock, usec, accuracy_us, callback, false)
    }

    /// Like [`Event::add_time`] but `usec` is interpreted relative to the
    /// current iteration's time. Wraps `sd_event_add_time_relative(3)`.
    pub fn add_time_relative<F>(
        &self,
        clock: clockid_t,
        usec: u64,
        accuracy_us: u64,
        callback: F,
    ) -> io::Result<EventSource>
    where
        F: FnMut(u64) -> i32 + 'static,
    {
        self.add_time_impl(clock, usec, accuracy_us, callback, true)
    }

    fn add_time_impl<F>(
        &self,
        clock: clockid_t,
        usec: u64,
        accuracy_us: u64,
        callback: F,
        relative: bool,
    ) -> io::Result<EventSource>
    where
        F: FnMut(u64) -> i32 + 'static,
    {
        let state = Box::new(SourceState {
            handler_time: RefCell::new(Some(Box::new(callback))),
            ..Default::default()
        });
        let userdata = Box::into_raw(state);
        let mut src: *mut sys::sd_event_source = ptr::null_mut();
        let func = if relative {
            sys::sd_event_add_time_relative
        } else {
            sys::sd_event_add_time
        };
        let r = unsafe {
            func(
                self.as_ptr(),
                &mut src,
                clock,
                usec,
                accuracy_us,
                Some(time_trampoline),
                userdata as *mut c_void,
            )
        };
        finish_source(r, src, userdata)
    }

    /// Add a UNIX signal source for `sig` (e.g. `libc::SIGTERM`).
    ///
    /// The signal must be blocked in **all** threads before the call. OR
    /// [`SIGNAL_PROCMASK`] into `sig` to have libsystemd do it for the
    /// calling thread. Wraps `sd_event_add_signal(3)`.
    pub fn add_signal<F>(&self, sig: c_int, callback: F) -> io::Result<EventSource>
    where
        F: FnMut(&signalfd_siginfo) -> i32 + 'static,
    {
        let state = Box::new(SourceState {
            handler_signal: RefCell::new(Some(Box::new(callback))),
            ..Default::default()
        });
        let userdata = Box::into_raw(state);
        let mut src: *mut sys::sd_event_source = ptr::null_mut();
        let r = unsafe {
            sys::sd_event_add_signal(
                self.as_ptr(),
                &mut src,
                sig,
                Some(signal_trampoline),
                userdata as *mut c_void,
            )
        };
        finish_source(r, src, userdata)
    }

    /// Watch a direct child process for state changes.
    ///
    /// `options` is the same flag set as `waitid(2)` (e.g. `WEXITED |
    /// WSTOPPED`). The kernel's autoreap must be disabled; the loop reaps
    /// the child itself after the handler runs. Default enable mode is
    /// [`Enabled::OneShot`]. Wraps `sd_event_add_child(3)`.
    pub fn add_child<F>(&self, pid: pid_t, options: c_int, callback: F) -> io::Result<EventSource>
    where
        F: FnMut(&siginfo_t) -> i32 + 'static,
    {
        let state = Box::new(SourceState {
            handler_child: RefCell::new(Some(Box::new(callback))),
            ..Default::default()
        });
        let userdata = Box::into_raw(state);
        let mut src: *mut sys::sd_event_source = ptr::null_mut();
        let r = unsafe {
            sys::sd_event_add_child(
                self.as_ptr(),
                &mut src,
                pid,
                options,
                Some(child_trampoline),
                userdata as *mut c_void,
            )
        };
        finish_source(r, src, userdata)
    }

    /// Watch a child via a pidfd (see `pidfd_open(2)`).
    ///
    /// The pidfd is **not** closed by default; use
    /// [`EventSource::set_child_pidfd_own`] to hand over ownership. Wraps
    /// `sd_event_add_child_pidfd(3)`.
    pub fn add_child_pidfd<F>(
        &self,
        pidfd: RawFd,
        options: c_int,
        callback: F,
    ) -> io::Result<EventSource>
    where
        F: FnMut(&siginfo_t) -> i32 + 'static,
    {
        let state = Box::new(SourceState {
            handler_child: RefCell::new(Some(Box::new(callback))),
            ..Default::default()
        });
        let userdata = Box::into_raw(state);
        let mut src: *mut sys::sd_event_source = ptr::null_mut();
        let r = unsafe {
            sys::sd_event_add_child_pidfd(
                self.as_ptr(),
                &mut src,
                pidfd,
                options,
                Some(child_trampoline),
                userdata as *mut c_void,
            )
        };
        finish_source(r, src, userdata)
    }

    /// Add an inotify watch on `path` with the given mask
    /// (see `inotify(7)`; `IN_MASK_ADD` is rejected).
    ///
    /// Sources on the same inode share one underlying watch. Wraps
    /// `sd_event_add_inotify(3)`.
    pub fn add_inotify<F>(&self, path: &CStr, mask: u32, callback: F) -> io::Result<EventSource>
    where
        F: FnMut(&InotifyEvent) -> i32 + 'static,
    {
        let state = Box::new(SourceState {
            handler_inotify: RefCell::new(Some(Box::new(callback))),
            ..Default::default()
        });
        let userdata = Box::into_raw(state);
        let mut src: *mut sys::sd_event_source = ptr::null_mut();
        let r = unsafe {
            sys::sd_event_add_inotify(
                self.as_ptr(),
                &mut src,
                path.as_ptr(),
                mask,
                Some(inotify_trampoline),
                userdata as *mut c_void,
            )
        };
        finish_source(r, src, userdata)
    }

    /// Like [`Event::add_inotify`] but takes an already-open (typically
    /// `O_PATH`) fd. Wraps `sd_event_add_inotify_fd(3)`.
    pub fn add_inotify_fd<F>(&self, fd: RawFd, mask: u32, callback: F) -> io::Result<EventSource>
    where
        F: FnMut(&InotifyEvent) -> i32 + 'static,
    {
        let state = Box::new(SourceState {
            handler_inotify: RefCell::new(Some(Box::new(callback))),
            ..Default::default()
        });
        let userdata = Box::into_raw(state);
        let mut src: *mut sys::sd_event_source = ptr::null_mut();
        let r = unsafe {
            sys::sd_event_add_inotify_fd(
                self.as_ptr(),
                &mut src,
                fd,
                mask,
                Some(inotify_trampoline),
                userdata as *mut c_void,
            )
        };
        finish_source(r, src, userdata)
    }

    /// Add a deferred source that fires exactly once before the loop next
    /// sleeps. Default enable mode is [`Enabled::OneShot`]. Wraps
    /// `sd_event_add_defer(3)`.
    pub fn add_defer<F>(&self, callback: F) -> io::Result<EventSource>
    where
        F: FnMut() -> i32 + 'static,
    {
        self.add_simple(callback, sys::sd_event_add_defer)
    }

    /// Add a "post" source that runs before the loop sleeps, but only if at
    /// least one other (non-post) source was dispatched in the current
    /// iteration. Default enable mode is [`Enabled::On`]. Wraps
    /// `sd_event_add_post(3)`.
    pub fn add_post<F>(&self, callback: F) -> io::Result<EventSource>
    where
        F: FnMut() -> i32 + 'static,
    {
        self.add_simple(callback, sys::sd_event_add_post)
    }

    /// Add an exit source dispatched (in priority order) during loop
    /// shutdown, after [`Event::exit`] is called. Default enable mode is
    /// [`Enabled::OneShot`]. Wraps `sd_event_add_exit(3)`.
    pub fn add_exit<F>(&self, callback: F) -> io::Result<EventSource>
    where
        F: FnMut() -> i32 + 'static,
    {
        self.add_simple(callback, sys::sd_event_add_exit)
    }

    fn add_simple<F>(
        &self,
        callback: F,
        add: unsafe extern "C" fn(
            *mut sys::sd_event,
            *mut *mut sys::sd_event_source,
            sys::sd_event_handler_t,
            *mut c_void,
        ) -> c_int,
    ) -> io::Result<EventSource>
    where
        F: FnMut() -> i32 + 'static,
    {
        let state = Box::new(SourceState {
            handler_plain: RefCell::new(Some(Box::new(callback))),
            ..Default::default()
        });
        let userdata = Box::into_raw(state);
        let mut src: *mut sys::sd_event_source = ptr::null_mut();
        let r = unsafe {
            add(
                self.as_ptr(),
                &mut src,
                Some(plain_trampoline),
                userdata as *mut c_void,
            )
        };
        finish_source(r, src, userdata)
    }

    /// Add a memory-pressure source driven by Linux PSI events.
    ///
    /// Source resolution: `$MEMORY_PRESSURE_WATCH` → the calling process's
    /// cgroup `memory.pressure` → `/proc/pressure/memory`. Wraps
    /// `sd_event_add_memory_pressure(3)`.
    pub fn add_memory_pressure<F>(&self, callback: F) -> io::Result<EventSource>
    where
        F: FnMut() -> i32 + 'static,
    {
        self.add_simple(callback, sys::sd_event_add_memory_pressure)
    }

    /// Add a memory-pressure source using libsystemd's default handler,
    /// which calls [`trim_memory`]. Wraps `sd_event_add_memory_pressure(3)`
    /// with a NULL handler.
    pub fn add_memory_pressure_default(&self) -> io::Result<EventSource> {
        let state = Box::new(SourceState::default());
        let userdata = Box::into_raw(state);
        let mut src: *mut sys::sd_event_source = ptr::null_mut();
        let r = unsafe {
            sys::sd_event_add_memory_pressure(
                self.as_ptr(),
                &mut src,
                None,
                userdata as *mut c_void,
            )
        };
        finish_source(r, src, userdata)
    }
}

impl Clone for Event {
    fn clone(&self) -> Self {
        unsafe { sys::sd_event_ref(self.as_ptr()) };
        Self {
            inner: self.inner,
            _not_send: PhantomData,
        }
    }
}

impl Drop for Event {
    fn drop(&mut self) {
        unsafe {
            sys::sd_event_unref(self.as_ptr());
        }
    }
}

/// Release libsystemd's internal allocation caches and call `malloc_trim(3)`.
///
/// Wraps `sd_event_trim_memory(3)`.
pub fn trim_memory() -> io::Result<()> {
    check(unsafe { sys::sd_event_trim_memory() }).map(|_| ())
}
