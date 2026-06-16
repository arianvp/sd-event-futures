//! Sockets: non-blocking connects, accepting, splicing, and passing file
//! descriptors.

use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6, TcpStream};
use std::os::fd::{AsRawFd, BorrowedFd, FromRawFd, OwnedFd, RawFd};
use std::os::linux::net::SocketAddrExt;
use std::os::unix::net::{SocketAddr as UnixSocketAddr, UnixStream};

use super::cvt;

/// Move up to `len` bytes from `from` to `to` inside the kernel; one of
/// them must be a pipe. Never blocks.
pub fn splice(from: BorrowedFd<'_>, to: BorrowedFd<'_>, len: usize) -> io::Result<usize> {
    let flags = libc::SPLICE_F_MOVE | libc::SPLICE_F_NONBLOCK;
    // SAFETY: null offsets are allowed and mean "the file position".
    let n = unsafe {
        libc::splice(
            from.as_raw_fd(),
            std::ptr::null_mut(),
            to.as_raw_fd(),
            std::ptr::null_mut(),
            len,
            flags,
        )
    };
    cvt(n).map(|n| n as usize)
}

/// `send(2)` without `SIGPIPE` when the peer is gone.
pub fn send(socket: BorrowedFd<'_>, data: &[u8]) -> io::Result<usize> {
    // SAFETY: `data` is readable for its length.
    let n = unsafe {
        libc::send(
            socket.as_raw_fd(),
            data.as_ptr().cast(),
            data.len(),
            libc::MSG_NOSIGNAL,
        )
    };
    cvt(n).map(|n| n as usize)
}

/// Shut a socket down for writing, so the peer sees end of file.
pub fn shutdown_write(socket: BorrowedFd<'_>) -> io::Result<()> {
    // SAFETY: no pointers involved.
    cvt(unsafe { libc::shutdown(socket.as_raw_fd(), libc::SHUT_WR) }).map(drop)
}

/// The next connection on a listening socket, non-blocking itself.
pub fn accept(listener: BorrowedFd<'_>) -> io::Result<OwnedFd> {
    let flags = libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC;
    // SAFETY: null address arguments are allowed.
    let fd = cvt(unsafe {
        libc::accept4(
            listener.as_raw_fd(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            flags,
        )
    })?;
    // SAFETY: `accept4()` just opened it, and nothing else owns it.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

fn socket_option(socket: BorrowedFd<'_>, name: libc::c_int) -> io::Result<libc::c_int> {
    let mut value: libc::c_int = 0;
    let mut len = size_of::<libc::c_int>() as libc::socklen_t;
    // SAFETY: `value` is an int-sized buffer and `len` says so.
    cvt(unsafe {
        libc::getsockopt(
            socket.as_raw_fd(),
            libc::SOL_SOCKET,
            name,
            (&raw mut value).cast(),
            &mut len,
        )
    })?;
    Ok(value)
}

/// Whether `socket` is a stream socket that is listening, like
/// `sd_is_socket(fd, AF_UNSPEC, SOCK_STREAM, 1)`.
pub fn is_listening_stream(socket: BorrowedFd<'_>) -> io::Result<bool> {
    Ok(socket_option(socket, libc::SO_TYPE)? == libc::SOCK_STREAM
        && socket_option(socket, libc::SO_ACCEPTCONN)? != 0)
}

type NameFn =
    unsafe extern "C" fn(libc::c_int, *mut libc::sockaddr, *mut libc::socklen_t) -> libc::c_int;

fn inet_name(socket: BorrowedFd<'_>, name: NameFn) -> Option<SocketAddr> {
    let mut storage = std::mem::MaybeUninit::<libc::sockaddr_storage>::zeroed();
    let mut len = size_of::<libc::sockaddr_storage>() as libc::socklen_t;
    // SAFETY: `storage` has room for any address, `len` says how much.
    cvt(unsafe { name(socket.as_raw_fd(), storage.as_mut_ptr().cast(), &mut len) }).ok()?;
    // SAFETY: zeroed is a valid `sockaddr_storage`, now filled in.
    to_socket_addr(&unsafe { storage.assume_init() })
}

/// The address of the other end of an IPv4 or IPv6 socket.
pub fn peer_address(socket: BorrowedFd<'_>) -> Option<SocketAddr> {
    inet_name(socket, libc::getpeername)
}

/// The address an IPv4 or IPv6 socket is bound to.
pub fn local_address(socket: BorrowedFd<'_>) -> Option<SocketAddr> {
    inet_name(socket, libc::getsockname)
}

pub(super) fn to_socket_addr(storage: &libc::sockaddr_storage) -> Option<SocketAddr> {
    match libc::c_int::from(storage.ss_family) {
        libc::AF_INET => {
            // SAFETY: the family says this is a `sockaddr_in`, which fits.
            let sin =
                unsafe { &*(storage as *const libc::sockaddr_storage).cast::<libc::sockaddr_in>() };
            let ip = Ipv4Addr::from(u32::from_be(sin.sin_addr.s_addr));
            Some(SocketAddrV4::new(ip, u16::from_be(sin.sin_port)).into())
        }
        libc::AF_INET6 => {
            // SAFETY: the family says this is a `sockaddr_in6`, which fits.
            let sin6 = unsafe {
                &*(storage as *const libc::sockaddr_storage).cast::<libc::sockaddr_in6>()
            };
            let ip = Ipv6Addr::from(sin6.sin6_addr.s6_addr);
            Some(
                SocketAddrV6::new(
                    ip,
                    u16::from_be(sin6.sin6_port),
                    sin6.sin6_flowinfo,
                    sin6.sin6_scope_id,
                )
                .into(),
            )
        }
        _ => None,
    }
}

fn to_sockaddr(addr: SocketAddr) -> (libc::sockaddr_storage, libc::socklen_t) {
    // SAFETY: all zeroes is a valid `sockaddr_storage`.
    let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    let len = match addr {
        SocketAddr::V4(a) => {
            let sin = libc::sockaddr_in {
                sin_family: libc::AF_INET as libc::sa_family_t,
                sin_port: a.port().to_be(),
                sin_addr: libc::in_addr {
                    s_addr: u32::from(*a.ip()).to_be(),
                },
                sin_zero: [0; 8],
            };
            // SAFETY: a `sockaddr_in` fits in a `sockaddr_storage`.
            unsafe { (&raw mut storage).cast::<libc::sockaddr_in>().write(sin) };
            size_of::<libc::sockaddr_in>()
        }
        SocketAddr::V6(a) => {
            let sin6 = libc::sockaddr_in6 {
                sin6_family: libc::AF_INET6 as libc::sa_family_t,
                sin6_port: a.port().to_be(),
                sin6_flowinfo: a.flowinfo(),
                sin6_addr: libc::in6_addr {
                    s6_addr: a.ip().octets(),
                },
                sin6_scope_id: a.scope_id(),
            };
            // SAFETY: a `sockaddr_in6` fits in a `sockaddr_storage`.
            unsafe { (&raw mut storage).cast::<libc::sockaddr_in6>().write(sin6) };
            size_of::<libc::sockaddr_in6>()
        }
    };
    (storage, len as libc::socklen_t)
}

fn stream_socket(family: libc::c_int) -> io::Result<OwnedFd> {
    let kind = libc::SOCK_STREAM | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC;
    // SAFETY: no pointers involved.
    let fd = cvt(unsafe { libc::socket(family, kind, 0) })?;
    // SAFETY: `socket()` just opened it, and nothing else owns it.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

fn connect(socket: RawFd, address: *const libc::sockaddr, len: libc::socklen_t) -> io::Result<()> {
    // SAFETY: callers pass an address that is valid for `len` bytes.
    cvt(unsafe { libc::connect(socket, address, len) }).map(drop)
}

/// Start connecting a non-blocking TCP socket to `addr`. The connection is
/// up once the socket turns writable and `take_error()` has nothing.
pub fn start_connect(addr: SocketAddr) -> io::Result<TcpStream> {
    let family = if addr.is_ipv4() {
        libc::AF_INET
    } else {
        libc::AF_INET6
    };
    let socket = stream_socket(family)?;
    let (address, len) = to_sockaddr(addr);
    match connect(socket.as_raw_fd(), (&raw const address).cast(), len) {
        Err(e) if e.raw_os_error() != Some(libc::EINPROGRESS) => Err(e),
        _ => Ok(TcpStream::from(socket)),
    }
}

/// Connect a non-blocking Unix stream socket, by path or abstract name. A
/// full listen backlog is an error rather than something to wait for.
pub fn connect_unix(addr: &UnixSocketAddr) -> io::Result<UnixStream> {
    // SAFETY: all zeroes is a valid `sockaddr_un`.
    let mut sun: libc::sockaddr_un = unsafe { std::mem::zeroed() };
    sun.sun_family = libc::AF_UNIX as libc::sa_family_t;
    // Abstract names start with a NUL and are counted, paths end with one.
    let (name, offset, terminator) = match (addr.as_pathname(), addr.as_abstract_name()) {
        (Some(path), _) => (path.as_os_str().as_encoded_bytes(), 0, 1),
        (None, Some(name)) => (name, 1, 0),
        (None, None) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "unnamed Unix socket address",
            ));
        }
    };
    if offset + name.len() + terminator > sun.sun_path.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Unix socket path too long",
        ));
    }
    for (dst, &src) in sun.sun_path[offset..].iter_mut().zip(name) {
        *dst = src as libc::c_char;
    }
    let len = std::mem::offset_of!(libc::sockaddr_un, sun_path) + offset + name.len() + terminator;

    let socket = stream_socket(libc::AF_UNIX)?;
    connect(
        socket.as_raw_fd(),
        (&raw const sun).cast(),
        len as libc::socklen_t,
    )?;
    Ok(UnixStream::from(socket))
}

/// Send `data` with `fds` attached (`SCM_RIGHTS`) on a connected socket.
pub fn send_with_fds(
    socket: BorrowedFd<'_>,
    data: &[u8],
    fds: &[BorrowedFd<'_>],
) -> io::Result<()> {
    let fds: Vec<RawFd> = fds.iter().map(AsRawFd::as_raw_fd).collect();
    let payload = size_of_val(fds.as_slice()) as u32;
    let mut iov = libc::iovec {
        iov_base: data.as_ptr() as *mut _,
        iov_len: data.len(),
    };
    // SAFETY: a zeroed `msghdr` is valid: no name, no control data.
    let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;

    // SAFETY: pure arithmetic.
    let space = unsafe { libc::CMSG_SPACE(payload) } as usize;
    // u64s, so the buffer is aligned for `cmsghdr`.
    let mut control = vec![0u64; space.div_ceil(size_of::<u64>())];
    if !fds.is_empty() {
        msg.msg_control = control.as_mut_ptr().cast();
        msg.msg_controllen = space;
        // SAFETY: the control buffer has room for one header with `payload`
        // bytes of data, so the first header exists and its data fits.
        unsafe {
            let cmsg = libc::CMSG_FIRSTHDR(&msg);
            (*cmsg).cmsg_level = libc::SOL_SOCKET;
            (*cmsg).cmsg_type = libc::SCM_RIGHTS;
            (*cmsg).cmsg_len = libc::CMSG_LEN(payload) as usize;
            std::ptr::copy_nonoverlapping(
                fds.as_ptr().cast::<u8>(),
                libc::CMSG_DATA(cmsg),
                payload as usize,
            );
        }
    }
    // SAFETY: `msg` points at `iov` and `control`, which outlive the call.
    cvt(unsafe { libc::sendmsg(socket.as_raw_fd(), &msg, libc::MSG_NOSIGNAL) }).map(drop)
}

/// Receive one message into `buf`, with any file descriptors attached.
pub fn recv_with_fds(socket: BorrowedFd<'_>, buf: &mut [u8]) -> io::Result<(usize, Vec<OwnedFd>)> {
    let mut iov = libc::iovec {
        iov_base: buf.as_mut_ptr().cast(),
        iov_len: buf.len(),
    };
    let mut control = [0u64; 64];
    // SAFETY: a zeroed `msghdr` is valid.
    let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = control.as_mut_ptr().cast();
    msg.msg_controllen = size_of_val(&control);
    // SAFETY: `msg` points at `iov` and `control`, which outlive the call.
    let n = cvt(unsafe { libc::recvmsg(socket.as_raw_fd(), &mut msg, libc::MSG_CMSG_CLOEXEC) })?;

    let mut fds = Vec::new();
    // SAFETY: the kernel filled in `msg_controllen` bytes of headers, which
    // the CMSG_* macros walk without leaving them; each `SCM_RIGHTS`
    // header carries descriptors that are now ours.
    unsafe {
        let mut cmsg = libc::CMSG_FIRSTHDR(&msg);
        while !cmsg.is_null() {
            if (*cmsg).cmsg_level == libc::SOL_SOCKET && (*cmsg).cmsg_type == libc::SCM_RIGHTS {
                let count = ((*cmsg).cmsg_len - libc::CMSG_LEN(0) as usize) / size_of::<RawFd>();
                let data = libc::CMSG_DATA(cmsg).cast::<RawFd>();
                fds.extend((0..count).map(|i| OwnedFd::from_raw_fd(data.add(i).read_unaligned())));
            }
            cmsg = libc::CMSG_NXTHDR(&msg, cmsg);
        }
    }
    Ok((n as usize, fds))
}
