//! `AF_VSOCK`, the socket family between virtual machines and their host.

use std::io;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd};
use std::os::raw::c_int;

use super::cvt;

/// A vsock address: a context ID (`VMADDR_CID_*`) and a port.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct VsockAddr {
    pub cid: u32,
    pub port: u32,
}

impl VsockAddr {
    /// `port` on whichever context ID is local, for binding.
    pub fn any_cid(port: u32) -> Self {
        Self {
            cid: libc::VMADDR_CID_ANY,
            port,
        }
    }

    fn raw(self) -> libc::sockaddr_vm {
        // SAFETY: all zeroes is a valid `sockaddr_vm`.
        let mut address: libc::sockaddr_vm = unsafe { std::mem::zeroed() };
        address.svm_family = libc::AF_VSOCK as libc::sa_family_t;
        address.svm_cid = self.cid;
        address.svm_port = self.port;
        address
    }
}

const LEN: libc::socklen_t = size_of::<libc::sockaddr_vm>() as libc::socklen_t;

fn socket(kind: c_int) -> io::Result<OwnedFd> {
    // SAFETY: no pointers involved.
    let fd = cvt(unsafe { libc::socket(libc::AF_VSOCK, kind | libc::SOCK_CLOEXEC, 0) })?;
    // SAFETY: `socket()` just opened it, and nothing else owns it.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

fn bind(socket: BorrowedFd<'_>, address: VsockAddr) -> io::Result<()> {
    let address = address.raw();
    // SAFETY: `address` is a `sockaddr_vm` of `LEN` bytes.
    cvt(unsafe { libc::bind(socket.as_raw_fd(), (&raw const address).cast(), LEN) }).map(drop)
}

/// Send `data` to `to` over a socket of type `kind`, connecting first for
/// stream and sequential-packet sockets.
///
/// The source port is bound below 1024 if possible, which proves to the
/// host that the message comes from a privileged process; unprivileged
/// senders just send from wherever they are.
pub fn vsock_send(to: VsockAddr, kind: c_int, data: &[u8]) -> io::Result<()> {
    let socket = socket(kind)?;
    for port in (1..1024).rev() {
        match bind(socket.as_fd(), VsockAddr::any_cid(port)) {
            Err(e) if e.raw_os_error() == Some(libc::EADDRINUSE) => continue,
            _ => break,
        }
    }
    let address = to.raw();
    let address_ptr = (&raw const address).cast();
    let sent = if kind == libc::SOCK_DGRAM {
        // SAFETY: `data` is readable for its length, `address` is `LEN` bytes.
        unsafe {
            libc::sendto(
                socket.as_raw_fd(),
                data.as_ptr().cast(),
                data.len(),
                libc::MSG_NOSIGNAL,
                address_ptr,
                LEN,
            )
        }
    } else {
        // SAFETY: `address` is a `sockaddr_vm` of `LEN` bytes.
        cvt(unsafe { libc::connect(socket.as_raw_fd(), address_ptr, LEN) })?;
        // SAFETY: `data` is readable for its length.
        unsafe {
            libc::send(
                socket.as_raw_fd(),
                data.as_ptr().cast(),
                data.len(),
                libc::MSG_NOSIGNAL,
            )
        }
    };
    cvt(sent).map(drop)
}

/// A listening vsock socket of type `kind` on any port, and that port.
pub fn vsock_listen(kind: c_int) -> io::Result<(OwnedFd, u32)> {
    let socket = socket(kind)?;
    bind(socket.as_fd(), VsockAddr::any_cid(libc::VMADDR_PORT_ANY))?;
    // SAFETY: no pointers involved.
    cvt(unsafe { libc::listen(socket.as_raw_fd(), 16) })?;
    let mut address = VsockAddr { cid: 0, port: 0 }.raw();
    let mut len = LEN;
    // SAFETY: `address` has room for a `sockaddr_vm`, `len` says so.
    cvt(unsafe { libc::getsockname(socket.as_raw_fd(), (&raw mut address).cast(), &mut len) })?;
    Ok((socket, address.svm_port))
}
