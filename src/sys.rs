//! Raw FFI bindings for libsystemd's `sd-event`. See `man 3 sd-event`.
//!
//! All declarations here are hand-written from `/usr/include/systemd/sd-event.h`.

#![allow(non_camel_case_types)]

use std::os::raw::{c_char, c_int, c_uint, c_void};

use libc::{clockid_t, inotify_event, pid_t, siginfo_t, signalfd_siginfo};

pub enum sd_event {}
pub enum sd_event_source {}

pub const SD_EVENT_OFF: c_int = 0;
pub const SD_EVENT_ON: c_int = 1;
pub const SD_EVENT_ONESHOT: c_int = -1;

pub const SD_EVENT_INITIAL: c_int = 0;
pub const SD_EVENT_ARMED: c_int = 1;
pub const SD_EVENT_PENDING: c_int = 2;
pub const SD_EVENT_RUNNING: c_int = 3;
pub const SD_EVENT_EXITING: c_int = 4;
pub const SD_EVENT_FINISHED: c_int = 5;
pub const SD_EVENT_PREPARING: c_int = 6;

pub const SD_EVENT_PRIORITY_IMPORTANT: i64 = -100;
pub const SD_EVENT_PRIORITY_NORMAL: i64 = 0;
pub const SD_EVENT_PRIORITY_IDLE: i64 = 100;

/// OR with the signal number to make `sd_event_add_signal` block the signal
/// in the calling thread automatically.
pub const SD_EVENT_SIGNAL_PROCMASK: c_int = 1 << 30;

pub type sd_event_handler_t =
    Option<unsafe extern "C" fn(s: *mut sd_event_source, userdata: *mut c_void) -> c_int>;
pub type sd_event_io_handler_t = Option<
    unsafe extern "C" fn(
        s: *mut sd_event_source,
        fd: c_int,
        revents: u32,
        userdata: *mut c_void,
    ) -> c_int,
>;
pub type sd_event_time_handler_t = Option<
    unsafe extern "C" fn(s: *mut sd_event_source, usec: u64, userdata: *mut c_void) -> c_int,
>;
pub type sd_event_signal_handler_t = Option<
    unsafe extern "C" fn(
        s: *mut sd_event_source,
        si: *const signalfd_siginfo,
        userdata: *mut c_void,
    ) -> c_int,
>;
pub type sd_event_child_handler_t = Option<
    unsafe extern "C" fn(
        s: *mut sd_event_source,
        si: *const siginfo_t,
        userdata: *mut c_void,
    ) -> c_int,
>;
pub type sd_event_inotify_handler_t = Option<
    unsafe extern "C" fn(
        s: *mut sd_event_source,
        event: *const inotify_event,
        userdata: *mut c_void,
    ) -> c_int,
>;
pub type sd_event_destroy_t = Option<unsafe extern "C" fn(userdata: *mut c_void)>;

unsafe extern "C" {
    // Loop lifecycle
    pub fn sd_event_new(ret: *mut *mut sd_event) -> c_int;
    pub fn sd_event_default(ret: *mut *mut sd_event) -> c_int;
    pub fn sd_event_ref(e: *mut sd_event) -> *mut sd_event;
    pub fn sd_event_unref(e: *mut sd_event) -> *mut sd_event;
    pub fn sd_event_get_tid(e: *mut sd_event, ret: *mut pid_t) -> c_int;

    // Running the loop
    pub fn sd_event_loop(e: *mut sd_event) -> c_int;
    pub fn sd_event_run(e: *mut sd_event, timeout: u64) -> c_int;
    pub fn sd_event_prepare(e: *mut sd_event) -> c_int;
    pub fn sd_event_wait(e: *mut sd_event, timeout: u64) -> c_int;
    pub fn sd_event_dispatch(e: *mut sd_event) -> c_int;

    // Exiting
    pub fn sd_event_exit(e: *mut sd_event, code: c_int) -> c_int;
    pub fn sd_event_get_exit_code(e: *mut sd_event, ret: *mut c_int) -> c_int;

    // Integration / introspection
    pub fn sd_event_get_fd(e: *mut sd_event) -> c_int;
    pub fn sd_event_get_state(e: *mut sd_event) -> c_int;
    pub fn sd_event_get_iteration(e: *mut sd_event, ret: *mut u64) -> c_int;
    pub fn sd_event_now(e: *mut sd_event, clock: clockid_t, ret: *mut u64) -> c_int;

    // Loop knobs
    pub fn sd_event_set_exit_on_idle(e: *mut sd_event, b: c_int) -> c_int;
    pub fn sd_event_get_exit_on_idle(e: *mut sd_event) -> c_int;
    pub fn sd_event_set_signal_exit(e: *mut sd_event, b: c_int) -> c_int;
    pub fn sd_event_set_watchdog(e: *mut sd_event, b: c_int) -> c_int;
    pub fn sd_event_get_watchdog(e: *mut sd_event) -> c_int;
    pub fn sd_event_trim_memory() -> c_int;

    // Source constructors
    pub fn sd_event_add_io(
        e: *mut sd_event,
        ret: *mut *mut sd_event_source,
        fd: c_int,
        events: u32,
        callback: sd_event_io_handler_t,
        userdata: *mut c_void,
    ) -> c_int;
    pub fn sd_event_add_time(
        e: *mut sd_event,
        ret: *mut *mut sd_event_source,
        clock: clockid_t,
        usec: u64,
        accuracy: u64,
        callback: sd_event_time_handler_t,
        userdata: *mut c_void,
    ) -> c_int;
    pub fn sd_event_add_time_relative(
        e: *mut sd_event,
        ret: *mut *mut sd_event_source,
        clock: clockid_t,
        usec: u64,
        accuracy: u64,
        callback: sd_event_time_handler_t,
        userdata: *mut c_void,
    ) -> c_int;
    pub fn sd_event_add_signal(
        e: *mut sd_event,
        ret: *mut *mut sd_event_source,
        sig: c_int,
        callback: sd_event_signal_handler_t,
        userdata: *mut c_void,
    ) -> c_int;
    pub fn sd_event_add_child(
        e: *mut sd_event,
        ret: *mut *mut sd_event_source,
        pid: pid_t,
        options: c_int,
        callback: sd_event_child_handler_t,
        userdata: *mut c_void,
    ) -> c_int;
    pub fn sd_event_add_child_pidfd(
        e: *mut sd_event,
        ret: *mut *mut sd_event_source,
        pidfd: c_int,
        options: c_int,
        callback: sd_event_child_handler_t,
        userdata: *mut c_void,
    ) -> c_int;
    pub fn sd_event_add_inotify(
        e: *mut sd_event,
        ret: *mut *mut sd_event_source,
        path: *const c_char,
        mask: u32,
        callback: sd_event_inotify_handler_t,
        userdata: *mut c_void,
    ) -> c_int;
    pub fn sd_event_add_inotify_fd(
        e: *mut sd_event,
        ret: *mut *mut sd_event_source,
        fd: c_int,
        mask: u32,
        callback: sd_event_inotify_handler_t,
        userdata: *mut c_void,
    ) -> c_int;
    pub fn sd_event_add_defer(
        e: *mut sd_event,
        ret: *mut *mut sd_event_source,
        callback: sd_event_handler_t,
        userdata: *mut c_void,
    ) -> c_int;
    pub fn sd_event_add_post(
        e: *mut sd_event,
        ret: *mut *mut sd_event_source,
        callback: sd_event_handler_t,
        userdata: *mut c_void,
    ) -> c_int;
    pub fn sd_event_add_exit(
        e: *mut sd_event,
        ret: *mut *mut sd_event_source,
        callback: sd_event_handler_t,
        userdata: *mut c_void,
    ) -> c_int;
    pub fn sd_event_add_memory_pressure(
        e: *mut sd_event,
        ret: *mut *mut sd_event_source,
        callback: sd_event_handler_t,
        userdata: *mut c_void,
    ) -> c_int;

    // Source reference counting
    pub fn sd_event_source_ref(s: *mut sd_event_source) -> *mut sd_event_source;
    pub fn sd_event_source_unref(s: *mut sd_event_source) -> *mut sd_event_source;
    pub fn sd_event_source_disable_unref(s: *mut sd_event_source) -> *mut sd_event_source;

    // Source ops — generic
    pub fn sd_event_source_get_event(s: *mut sd_event_source) -> *mut sd_event;
    pub fn sd_event_source_set_description(
        s: *mut sd_event_source,
        description: *const c_char,
    ) -> c_int;
    pub fn sd_event_source_get_description(
        s: *mut sd_event_source,
        ret: *mut *const c_char,
    ) -> c_int;
    pub fn sd_event_source_set_userdata(
        s: *mut sd_event_source,
        userdata: *mut c_void,
    ) -> *mut c_void;
    pub fn sd_event_source_get_userdata(s: *mut sd_event_source) -> *mut c_void;
    pub fn sd_event_source_set_prepare(
        s: *mut sd_event_source,
        callback: sd_event_handler_t,
    ) -> c_int;
    pub fn sd_event_source_get_pending(s: *mut sd_event_source) -> c_int;
    pub fn sd_event_source_set_priority(s: *mut sd_event_source, priority: i64) -> c_int;
    pub fn sd_event_source_get_priority(s: *mut sd_event_source, ret: *mut i64) -> c_int;
    pub fn sd_event_source_set_enabled(s: *mut sd_event_source, enabled: c_int) -> c_int;
    pub fn sd_event_source_get_enabled(s: *mut sd_event_source, ret: *mut c_int) -> c_int;
    pub fn sd_event_source_set_destroy_callback(
        s: *mut sd_event_source,
        callback: sd_event_destroy_t,
    ) -> c_int;
    pub fn sd_event_source_get_destroy_callback(
        s: *mut sd_event_source,
        ret: *mut sd_event_destroy_t,
    ) -> c_int;
    pub fn sd_event_source_set_floating(s: *mut sd_event_source, b: c_int) -> c_int;
    pub fn sd_event_source_get_floating(s: *mut sd_event_source) -> c_int;
    pub fn sd_event_source_set_exit_on_failure(s: *mut sd_event_source, b: c_int) -> c_int;
    pub fn sd_event_source_get_exit_on_failure(s: *mut sd_event_source) -> c_int;

    // Rate limiting
    pub fn sd_event_source_set_ratelimit(
        s: *mut sd_event_source,
        interval_usec: u64,
        burst: c_uint,
    ) -> c_int;
    pub fn sd_event_source_get_ratelimit(
        s: *mut sd_event_source,
        ret_interval_usec: *mut u64,
        ret_burst: *mut c_uint,
    ) -> c_int;
    pub fn sd_event_source_is_ratelimited(s: *mut sd_event_source) -> c_int;
    pub fn sd_event_source_leave_ratelimit(s: *mut sd_event_source) -> c_int;
    pub fn sd_event_source_set_ratelimit_expire_callback(
        s: *mut sd_event_source,
        callback: sd_event_handler_t,
    ) -> c_int;

    // I/O source ops
    pub fn sd_event_source_get_io_fd(s: *mut sd_event_source) -> c_int;
    pub fn sd_event_source_set_io_fd(s: *mut sd_event_source, fd: c_int) -> c_int;
    pub fn sd_event_source_get_io_fd_own(s: *mut sd_event_source) -> c_int;
    pub fn sd_event_source_set_io_fd_own(s: *mut sd_event_source, own: c_int) -> c_int;
    pub fn sd_event_source_get_io_events(s: *mut sd_event_source, ret: *mut u32) -> c_int;
    pub fn sd_event_source_set_io_events(s: *mut sd_event_source, events: u32) -> c_int;
    pub fn sd_event_source_get_io_revents(s: *mut sd_event_source, ret: *mut u32) -> c_int;

    // Time source ops
    pub fn sd_event_source_get_time(s: *mut sd_event_source, ret: *mut u64) -> c_int;
    pub fn sd_event_source_set_time(s: *mut sd_event_source, usec: u64) -> c_int;
    pub fn sd_event_source_set_time_relative(s: *mut sd_event_source, usec: u64) -> c_int;
    pub fn sd_event_source_get_time_accuracy(s: *mut sd_event_source, ret: *mut u64) -> c_int;
    pub fn sd_event_source_set_time_accuracy(s: *mut sd_event_source, usec: u64) -> c_int;
    pub fn sd_event_source_get_time_clock(s: *mut sd_event_source, ret: *mut clockid_t) -> c_int;

    // Signal source ops
    pub fn sd_event_source_get_signal(s: *mut sd_event_source) -> c_int;

    // Child source ops
    pub fn sd_event_source_get_child_pid(s: *mut sd_event_source, ret: *mut pid_t) -> c_int;
    pub fn sd_event_source_get_child_pidfd(s: *mut sd_event_source) -> c_int;
    pub fn sd_event_source_get_child_pidfd_own(s: *mut sd_event_source) -> c_int;
    pub fn sd_event_source_set_child_pidfd_own(s: *mut sd_event_source, own: c_int) -> c_int;
    pub fn sd_event_source_get_child_process_own(s: *mut sd_event_source) -> c_int;
    pub fn sd_event_source_set_child_process_own(s: *mut sd_event_source, own: c_int) -> c_int;
    pub fn sd_event_source_send_child_signal(
        s: *mut sd_event_source,
        sig: c_int,
        si: *const siginfo_t,
        flags: c_uint,
    ) -> c_int;

    // Inotify source ops
    pub fn sd_event_source_get_inotify_mask(s: *mut sd_event_source, ret: *mut u32) -> c_int;
    pub fn sd_event_source_get_inotify_path(
        s: *mut sd_event_source,
        ret: *mut *const c_char,
    ) -> c_int;

    // Memory pressure source ops
    pub fn sd_event_source_set_memory_pressure_type(
        s: *mut sd_event_source,
        ty: *const c_char,
    ) -> c_int;
    pub fn sd_event_source_set_memory_pressure_period(
        s: *mut sd_event_source,
        threshold_usec: u64,
        window_usec: u64,
    ) -> c_int;
}
