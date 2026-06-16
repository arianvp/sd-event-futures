//! Safe wrappers for the system calls this crate makes outside of
//! `sd-event`.
//!
//! Each function makes one call, checks its result, and hands out owned
//! types (`OwnedFd`, `TcpStream`, …), so that the rest of the crate, and
//! the programs built on it, stay free of `unsafe`. Every `unsafe` block
//! that talks to libc directly lives in this module, next to the reason
//! it is sound.

mod fd;
mod net;
mod process;
mod signal;
mod socket;
mod vsock;

use std::io;

pub use fd::{
    EventFd, file_identity, mkfifo, nonblocking_pipe, pipe, renumber_fds, resize_pipe, set_cloexec,
    set_nonblocking, wait_readable,
};
pub use net::resolve;
pub use process::{
    Forked, close_fds_except, close_in_child, exit_now, exit_status, fork, gettid, kill,
    own_pidfd_id, pidfd_id, pidfd_open, pidfd_send_signal, pidfd_wait, process_exists, sync,
    thread_count, thread_exists, waitpid,
};
pub use signal::{is_blocked, raise, unblock_all_signals, with_all_signals_blocked};
pub use socket::{
    accept, connect_unix, is_listening_stream, local_address, peer_address, recv_with_fds, send,
    send_with_fds, shutdown_write, splice, start_connect,
};
pub use vsock::{VsockAddr, vsock_listen, vsock_send};

/// Return values that signal failure with `-1` and `errno`.
trait MinusOne: Copy + PartialEq {
    const MINUS_ONE: Self;
}

impl MinusOne for i32 {
    const MINUS_ONE: Self = -1;
}

impl MinusOne for isize {
    const MINUS_ONE: Self = -1;
}

impl MinusOne for i64 {
    const MINUS_ONE: Self = -1;
}

/// `Err(errno)` for `-1`, the value itself otherwise.
fn cvt<T: MinusOne>(r: T) -> io::Result<T> {
    if r == T::MINUS_ONE {
        Err(io::Error::last_os_error())
    } else {
        Ok(r)
    }
}

/// `e` the way systemd's `%m` prints it: the `strerror(3)` text, without
/// the " (os error N)" Rust adds.
pub fn errno_message(e: &io::Error) -> String {
    let Some(errno) = e.raw_os_error() else {
        return e.to_string();
    };
    let mut buf = [0 as libc::c_char; 256];
    // SAFETY: `buf` is writable for its whole length, and on success holds
    // a NUL-terminated string.
    let found = unsafe { libc::strerror_r(errno, buf.as_mut_ptr(), buf.len()) } == 0;
    if !found {
        return format!("Unknown error {errno}");
    }
    // SAFETY: as above, `strerror_r()` left a NUL-terminated string.
    unsafe { std::ffi::CStr::from_ptr(buf.as_ptr()) }
        .to_string_lossy()
        .into_owned()
}

/// `CLOCK_MONOTONIC` in microseconds.
pub fn monotonic_usec() -> u64 {
    let mut now = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `now` is a valid `timespec` to write to; the clock exists.
    unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut now) };
    now.tv_sec as u64 * 1_000_000 + now.tv_nsec as u64 / 1_000
}

/// Eight random bytes from the kernel.
pub fn random_u64() -> u64 {
    let mut bytes = [0u8; 8];
    // SAFETY: `bytes` is writable for its length.
    unsafe { libc::getrandom(bytes.as_mut_ptr().cast(), bytes.len(), 0) };
    u64::from_ne_bytes(bytes)
}

/// Raise `RLIMIT_NOFILE` as close to the kernel's `fs.nr_open` as the hard
/// limit allows, like systemd's `rlimit_nofile_bump(-1)`.
pub fn raise_open_files_limit() {
    let nr_open: libc::rlim_t = std::fs::read_to_string("/proc/sys/fs/nr_open")
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(1024 * 1024);
    let wanted = libc::rlimit {
        rlim_cur: nr_open,
        rlim_max: nr_open,
    };
    // SAFETY: `wanted` is a valid `rlimit`.
    if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &wanted) } == 0 {
        return;
    }
    let mut current = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `current` is a valid `rlimit` to write to.
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut current) } == 0 {
        let closest = nr_open.min(current.rlim_max);
        let closest = libc::rlimit {
            rlim_cur: closest,
            rlim_max: closest,
        };
        // SAFETY: `closest` is a valid `rlimit`.
        unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &closest) };
    }
}
