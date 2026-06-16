//! File descriptors: pipes, eventfds and their flags.

use std::ffi::CString;
use std::io;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::time::Duration;

use super::cvt;

fn pipe_with(flags: libc::c_int) -> io::Result<(OwnedFd, OwnedFd)> {
    let mut fds = [0; 2];
    // SAFETY: `fds` has room for the two descriptors.
    cvt(unsafe { libc::pipe2(fds.as_mut_ptr(), flags | libc::O_CLOEXEC) })?;
    // SAFETY: `pipe2()` just opened both, and nothing else owns them.
    Ok(unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) })
}

/// A pipe, as `(read end, write end)`.
pub fn pipe() -> io::Result<(OwnedFd, OwnedFd)> {
    pipe_with(0)
}

/// A pipe whose ends never block, as `(read end, write end)`.
pub fn nonblocking_pipe() -> io::Result<(OwnedFd, OwnedFd)> {
    pipe_with(libc::O_NONBLOCK)
}

/// Ask for a pipe buffer of `size` bytes and return the size the kernel
/// granted, which may be smaller.
pub fn resize_pipe(pipe: BorrowedFd<'_>, size: usize) -> io::Result<usize> {
    let size = libc::c_int::try_from(size).unwrap_or(libc::c_int::MAX);
    // SAFETY: plain fcntl on a valid descriptor; failure only means the
    // size stays as it is.
    unsafe { libc::fcntl(pipe.as_raw_fd(), libc::F_SETPIPE_SZ, size) };
    // SAFETY: plain fcntl on a valid descriptor.
    cvt(unsafe { libc::fcntl(pipe.as_raw_fd(), libc::F_GETPIPE_SZ) }).map(|n| n as usize)
}

fn update_flags(
    fd: RawFd,
    get: libc::c_int,
    set: libc::c_int,
    flag: libc::c_int,
) -> io::Result<()> {
    // SAFETY: plain fcntl on a descriptor the caller borrows.
    let flags = cvt(unsafe { libc::fcntl(fd, get) })?;
    if flags & flag == 0 {
        // SAFETY: as above.
        cvt(unsafe { libc::fcntl(fd, set, flags | flag) })?;
    }
    Ok(())
}

pub fn set_nonblocking(fd: BorrowedFd<'_>) -> io::Result<()> {
    update_flags(
        fd.as_raw_fd(),
        libc::F_GETFL,
        libc::F_SETFL,
        libc::O_NONBLOCK,
    )
}

pub fn set_cloexec(fd: BorrowedFd<'_>) -> io::Result<()> {
    update_flags(
        fd.as_raw_fd(),
        libc::F_GETFD,
        libc::F_SETFD,
        libc::FD_CLOEXEC,
    )
}

/// The `(device, inode)` pair `fstat(2)` reports, which identifies what a
/// descriptor refers to, for example to compare it with `$JOURNAL_STREAM`.
pub fn file_identity(fd: BorrowedFd<'_>) -> io::Result<(u64, u64)> {
    let mut st = std::mem::MaybeUninit::<libc::stat>::uninit();
    // SAFETY: `st` is writable and `fstat()` fills it in on success.
    cvt(unsafe { libc::fstat(fd.as_raw_fd(), st.as_mut_ptr()) })?;
    // SAFETY: initialized by the successful `fstat()`.
    let st = unsafe { st.assume_init() };
    Ok((st.st_dev, st.st_ino))
}

pub fn mkfifo(path: &Path, mode: libc::mode_t) -> io::Result<()> {
    let path =
        CString::new(path.as_os_str().as_bytes()).map_err(|_| io::ErrorKind::InvalidInput)?;
    // SAFETY: `path` is a NUL-terminated string.
    cvt(unsafe { libc::mkfifo(path.as_ptr(), mode) }).map(drop)
}

/// Whether `fd` becomes readable within `timeout`.
pub fn wait_readable(fd: BorrowedFd<'_>, timeout: Duration) -> io::Result<bool> {
    let mut pfd = libc::pollfd {
        fd: fd.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    let millis = libc::c_int::try_from(timeout.as_millis()).unwrap_or(libc::c_int::MAX);
    // SAFETY: `pfd` is one valid `pollfd`.
    Ok(cvt(unsafe { libc::poll(&mut pfd, 1, millis) })? == 1)
}

/// A counter one thread can bump to wake another that watches it for
/// readability (`eventfd(2)`).
#[derive(Debug)]
pub struct EventFd(OwnedFd);

impl EventFd {
    /// A non-blocking eventfd.
    pub fn new() -> io::Result<Self> {
        // SAFETY: no pointers involved.
        let fd = cvt(unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) })?;
        // SAFETY: `eventfd()` just opened it, and nothing else owns it.
        Ok(Self(unsafe { OwnedFd::from_raw_fd(fd) }))
    }

    /// Make it readable. Never blocks.
    pub fn signal(&self) {
        let one: u64 = 1;
        // SAFETY: writes the eight bytes of `one`. Only fails once the
        // counter would overflow, when it is readable anyway.
        unsafe {
            libc::write(
                self.0.as_raw_fd(),
                (&raw const one).cast(),
                size_of::<u64>(),
            )
        };
    }

    /// Read and reset the counter: how often it was signalled since.
    /// `WouldBlock` if it wasn't.
    pub fn reset(&self) -> io::Result<u64> {
        let mut count: u64 = 0;
        // SAFETY: reads eight bytes into `count`.
        cvt(unsafe {
            libc::read(
                self.0.as_raw_fd(),
                (&raw mut count).cast(),
                size_of::<u64>(),
            )
        })?;
        Ok(count)
    }
}

impl AsFd for EventFd {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.0.as_fd()
    }
}

/// Make `fds[i]` available as descriptor `first + i`, without close-on-exec,
/// the way a service manager hands sockets to a service. For use between
/// `fork()` and `exec()`: makes only system calls, and allocates nothing.
pub fn renumber_fds(fds: &[RawFd], first: RawFd) -> io::Result<()> {
    const MAX: usize = 64;
    if fds.len() > MAX {
        return Err(io::ErrorKind::InvalidInput.into());
    }
    // Copy everything out of the way first, so that a source already
    // sitting at a target number isn't overwritten before it was moved.
    let mut copies = [0; MAX];
    for (copy, &fd) in copies.iter_mut().zip(fds) {
        // SAFETY: duplicates a descriptor the caller passes us.
        *copy = cvt(unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, first + MAX as RawFd) })?;
    }
    for (i, &copy) in copies[..fds.len()].iter().enumerate() {
        // SAFETY: replaces descriptor `first + i`, which the caller gives up.
        cvt(unsafe { libc::dup2(copy, first + i as RawFd) })?;
    }
    Ok(())
}
