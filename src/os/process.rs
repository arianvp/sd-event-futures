//! Forking, pidfds and waiting.

use std::io;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, IntoRawFd, OwnedFd, RawFd};
use std::os::unix::process::ExitStatusExt;
use std::process::ExitStatus;

use libc::pid_t;

use super::cvt;

/// Threads in this process.
pub fn thread_count() -> usize {
    std::fs::read_dir("/proc/self/task").map_or(0, Iterator::count)
}

/// The calling thread's id.
pub fn gettid() -> pid_t {
    // SAFETY: no pointers involved.
    unsafe { libc::gettid() }
}

/// Whether thread `tid` of this process still exists.
pub fn thread_exists(tid: pid_t) -> bool {
    std::fs::exists(format!("/proc/self/task/{tid}")).unwrap_or(false)
}

fn require_single_thread() -> io::Result<()> {
    match thread_count() {
        1 => Ok(()),
        n => Err(io::Error::other(format!(
            "refusing to fork with {n} threads running"
        ))),
    }
}

/// Which side of a `fork()` this is.
pub enum Forked {
    Child,
    Parent(pid_t),
}

/// `fork(2)`, for single-threaded processes only, as PID 1 is: the child
/// then is a complete copy and may do anything. Fails with more than one
/// thread running, where the child would only be allowed
/// async-signal-safe calls.
pub fn fork() -> io::Result<Forked> {
    require_single_thread()?;
    // SAFETY: with this the only thread, nothing can be holding a lock or
    // be halfway through an update in the child's copy.
    match cvt(unsafe { libc::fork() })? {
        0 => Ok(Forked::Child),
        pid => Ok(Forked::Parent(pid)),
    }
}

/// Close `fd` from a child process that shares this process's descriptor
/// table, as systemd's `asynchronous_close()` does, so a `close()` that
/// blocks (the last reference to a file on a stuck network filesystem,
/// say) never blocks this process. Returns the child's pid and pidfd.
pub fn close_in_child(fd: OwnedFd) -> io::Result<(pid_t, OwnedFd)> {
    #[repr(C)]
    #[derive(Default)]
    struct CloneArgs {
        flags: u64,
        pidfd: u64,
        child_tid: u64,
        parent_tid: u64,
        exit_signal: u64,
        stack: u64,
        stack_size: u64,
        tls: u64,
        set_tid: u64,
        set_tid_size: u64,
        cgroup: u64,
    }

    let mut pidfd: libc::c_int = -1;
    let args = CloneArgs {
        flags: (libc::CLONE_FILES | libc::CLONE_PIDFD) as u64,
        pidfd: (&raw mut pidfd) as u64,
        exit_signal: libc::SIGCHLD as u64,
        ..CloneArgs::default()
    };
    let raw = fd.into_raw_fd();
    // SAFETY: without CLONE_VM the child gets its own copy of memory, and
    // runs on a copy of this stack like after fork(). It makes only the two
    // system calls below, which need nothing from glibc's state.
    let pid = unsafe { libc::syscall(libc::SYS_clone3, &args, size_of::<CloneArgs>()) };
    match cvt(pid) {
        Ok(0) => {
            // SAFETY: the shared table still holds `raw`, which this closes.
            unsafe {
                libc::close(raw);
                libc::_exit(0)
            }
        }
        // SAFETY: CLONE_PIDFD stored a new descriptor that we now own.
        Ok(pid) => Ok((pid as pid_t, unsafe { OwnedFd::from_raw_fd(pidfd) })),
        Err(e) => {
            // SAFETY: `raw` came from `into_raw_fd()` above; close it here.
            drop(unsafe { OwnedFd::from_raw_fd(raw) });
            Err(e)
        }
    }
}

/// Leave the process right away, without running destructors or `atexit`
/// handlers: how a forked child ends.
pub fn exit_now(code: libc::c_int) -> ! {
    // SAFETY: `_exit()` is always safe to call.
    unsafe { libc::_exit(code) }
}

/// Close every descriptor from 3 up except `keep`, like systemd's
/// `FORK_CLOSE_ALL_FDS`.
pub fn close_fds_except(keep: &[BorrowedFd<'_>]) {
    let mut keep: Vec<u32> = keep.iter().map(|fd| fd.as_raw_fd() as u32).collect();
    keep.sort_unstable();
    let mut low = 3;
    for fd in keep.into_iter().filter(|&fd| fd >= 3).chain([u32::MAX]) {
        if fd > low {
            // SAFETY: closes descriptors this process owns but doesn't keep.
            unsafe { libc::syscall(libc::SYS_close_range, low, fd - 1, 0) };
        }
        low = fd.saturating_add(1);
    }
}

/// A descriptor for process `pid` (`pidfd_open(2)`).
pub fn pidfd_open(pid: pid_t) -> io::Result<OwnedFd> {
    // SAFETY: no pointers involved.
    let fd = cvt(unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) })?;
    // SAFETY: `pidfd_open()` just opened it, and nothing else owns it.
    Ok(unsafe { OwnedFd::from_raw_fd(fd as RawFd) })
}

/// The pidfs inode ID of the process behind `pidfd`. Unlike its pid, no
/// other process ever gets the same one while the system runs. Read from
/// its file handle, as systemd's `pidfd_get_inode_id()` does, which has
/// the full 64 bits on 32-bit systems too.
///
/// Makes only system calls, and allocates nothing.
pub fn pidfd_id(pidfd: BorrowedFd<'_>) -> io::Result<u64> {
    /// A `struct file_handle` with room for `MAX_HANDLE_SZ` bytes.
    #[repr(C)]
    struct FileHandle {
        bytes: libc::c_uint,
        kind: libc::c_int,
        data: [u64; 16],
    }

    let mut handle = FileHandle {
        bytes: size_of::<[u64; 16]>() as libc::c_uint,
        kind: 0,
        data: [0; 16],
    };
    let mut mount_id = 0;
    // SAFETY: `handle` is a `file_handle` whose `bytes` says how much room
    // follows it, and the path is an empty C string.
    cvt(unsafe {
        libc::name_to_handle_at(
            pidfd.as_raw_fd(),
            c"".as_ptr(),
            (&raw mut handle).cast(),
            &mut mount_id,
            libc::AT_EMPTY_PATH,
        )
    })?;
    match handle.bytes {
        8.. => Ok(handle.data[0]),
        _ => Err(io::Error::from_raw_os_error(libc::EBADMSG)),
    }
}

/// This process's pidfs inode ID, see [`pidfd_id`]. Makes only system
/// calls, and allocates nothing.
pub fn own_pidfd_id() -> io::Result<u64> {
    pidfd_id(pidfd_open(std::process::id() as pid_t)?.as_fd())
}

pub fn pidfd_send_signal(pidfd: BorrowedFd<'_>, signal: libc::c_int) -> io::Result<()> {
    // SAFETY: a null `siginfo_t` means "like kill()".
    cvt(unsafe {
        libc::syscall(
            libc::SYS_pidfd_send_signal,
            pidfd.as_raw_fd(),
            signal,
            std::ptr::null::<libc::siginfo_t>(),
            0,
        )
    })
    .map(drop)
}

/// Wait for the process behind `pidfd` to exit and reap it. Blocks.
pub fn pidfd_wait(pidfd: BorrowedFd<'_>) -> io::Result<()> {
    let mut info = std::mem::MaybeUninit::<libc::siginfo_t>::zeroed();
    // SAFETY: `info` has room for the result.
    cvt(unsafe {
        libc::waitid(
            libc::P_PIDFD,
            pidfd.as_raw_fd() as libc::id_t,
            info.as_mut_ptr(),
            libc::WEXITED,
        )
    })
    .map(drop)
}

pub fn kill(pid: pid_t, signal: libc::c_int) -> io::Result<()> {
    // SAFETY: no pointers involved.
    cvt(unsafe { libc::kill(pid, signal) }).map(drop)
}

/// Whether process `pid` exists, as a zombie too.
pub fn process_exists(pid: pid_t) -> bool {
    kill(pid, 0).map_or_else(|e| e.raw_os_error() != Some(libc::ESRCH), |()| true)
}

/// Wait for child `pid` to exit, reap it and return how it ended. Blocks.
pub fn waitpid(pid: pid_t) -> io::Result<ExitStatus> {
    let mut status = 0;
    // SAFETY: `status` is a valid int to write to.
    cvt(unsafe { libc::waitpid(pid, &mut status, 0) })?;
    Ok(ExitStatus::from_raw(status))
}

/// How a child ended, from the `siginfo_t` its `SIGCHLD` or `waitid()`
/// reported.
pub fn exit_status(info: &libc::siginfo_t) -> ExitStatus {
    // SAFETY: child state reports always fill in `si_status`.
    let status = unsafe { info.si_status() };
    ExitStatus::from_raw(match info.si_code {
        libc::CLD_EXITED => (status & 0xff) << 8,
        libc::CLD_DUMPED => status | 0x80,
        _ => status,
    })
}

/// Flush all filesystems to disk (`sync(2)`). Blocks, possibly for long.
pub fn sync() {
    // SAFETY: no pointers involved.
    unsafe { libc::sync() };
}
