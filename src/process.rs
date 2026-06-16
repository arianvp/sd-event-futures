//! Child processes, owned through pidfds.
//!
//! systemd moves work that must not block the main loop, and that may not
//! run on a thread (PID 1 has none), into short-lived child processes:
//! fork, do the work, exit, and let the loop pick up the exit status.
//! [`Child`] owns such a process; [`Child::wait`] awaits its exit through
//! an sd-event child source.

use std::io;
use std::mem::ManuallyDrop;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd};
use std::panic::{self, AssertUnwindSafe};
use std::process::ExitStatus;

use libc::{c_int, pid_t};

use crate::blocking;
use crate::executor::try_spawn;
use crate::os::{self, Forked};
use crate::reactor::child_pidfd;

/// A child process of this process.
///
/// Dropping a `Child` that has not been waited for kills it with `SIGKILL`
/// and reaps it, like systemd's `pidref_done_sigkill_wait()`: in the
/// background while an executor is running, so the loop never blocks on
/// it, and by waiting otherwise. Use [`Child::detach`] to leave it running
/// instead.
pub struct Child {
    pidfd: ManuallyDrop<OwnedFd>,
    pid: pid_t,
    status: Option<ExitStatus>,
    detached: bool,
}

impl Child {
    /// Fork a child that runs `f` and exits with its return value (101 if
    /// `f` panics). In the child, every file descriptor other than stdio and
    /// `keep_fds` is closed and the signal mask is cleared, like systemd's
    /// `FORK_CLOSE_ALL_FDS | FORK_RESET_SIGNALS`.
    ///
    /// Only possible while this process has a single thread, as PID 1 does;
    /// the child is then a complete copy, and `f` may do anything. Idle
    /// threads of the [`blocking`] pool are stopped first, so it only fails
    /// while a blocking job runs or other threads were started.
    pub fn fork(keep_fds: &[BorrowedFd<'_>], f: impl FnOnce() -> c_int) -> io::Result<Child> {
        blocking::retire_idle();
        match os::fork()? {
            Forked::Child => {
                os::close_fds_except(keep_fds);
                os::unblock_all_signals();
                let code = panic::catch_unwind(AssertUnwindSafe(f)).unwrap_or(101);
                os::exit_now(code)
            }
            Forked::Parent(pid) => match os::pidfd_open(pid) {
                Ok(pidfd) => Ok(Self::new(pid, pidfd)),
                Err(e) => {
                    let _ = os::kill(pid, libc::SIGKILL);
                    let _ = os::waitpid(pid);
                    Err(e)
                }
            },
        }
    }

    /// Close `fd` from a child process that shares this process's file
    /// descriptor table, like systemd's `asynchronous_close()`: the
    /// descriptor is gone from this process too once the child is done,
    /// and however long the `close()` takes, it never blocks this process.
    /// If no child can be started, `fd` is closed right here instead.
    pub fn close(fd: OwnedFd) -> io::Result<Child> {
        let (pid, pidfd) = os::close_in_child(fd)?;
        Ok(Self::new(pid, pidfd))
    }

    fn new(pid: pid_t, pidfd: OwnedFd) -> Self {
        Self {
            pidfd: ManuallyDrop::new(pidfd),
            pid,
            status: None,
            detached: false,
        }
    }

    pub fn id(&self) -> pid_t {
        self.pid
    }

    /// Wait for the child to exit and reap it. Returns at once if it was
    /// already waited for.
    pub async fn wait(&mut self) -> io::Result<ExitStatus> {
        if let Some(status) = self.status {
            return Ok(status);
        }
        // sd-event reaps the child right after reporting its exit.
        let info = child_pidfd(self.pidfd.as_raw_fd(), libc::WEXITED)?.await;
        let status = os::exit_status(&info);
        self.status = Some(status);
        Ok(status)
    }

    /// Send `signal` to the child.
    pub fn kill(&self, signal: c_int) -> io::Result<()> {
        os::pidfd_send_signal(self.pidfd.as_fd(), signal)
    }

    /// Stop owning the child without killing or reaping it. Whoever reaps
    /// this process's children (PID 1 does so for all of them) collects
    /// it.
    pub fn detach(mut self) {
        self.detached = true;
    }
}

impl Drop for Child {
    fn drop(&mut self) {
        // SAFETY: the field is not used again after this.
        let pidfd = unsafe { ManuallyDrop::take(&mut self.pidfd) };
        if self.status.is_some() || self.detached {
            return;
        }
        let _ = os::pidfd_send_signal(pidfd.as_fd(), libc::SIGKILL);
        // Outside an executor the reaper is dropped unpolled, which reaps
        // right away.
        try_spawn(Reaper(Some(pidfd)).reap());
    }
}

/// Collects a killed child.
struct Reaper(Option<OwnedFd>);

impl Reaper {
    async fn reap(mut self) {
        let Some(pidfd) = &self.0 else { return };
        if let Ok(exit) = child_pidfd(pidfd.as_raw_fd(), libc::WEXITED) {
            exit.await;
            self.0 = None;
        }
    }
}

impl Drop for Reaper {
    fn drop(&mut self) {
        if let Some(pidfd) = &self.0 {
            let _ = os::pidfd_wait(pidfd.as_fd());
        }
    }
}
