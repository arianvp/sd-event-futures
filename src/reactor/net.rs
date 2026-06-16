//! Connecting stream sockets without blocking the loop.

use std::io;
use std::net::{SocketAddr, TcpStream};
use std::os::unix::net::{SocketAddr as UnixSocketAddr, UnixStream};

use super::AsyncFd;
use crate::os;

/// Connect a TCP socket to `addr`: `connect(2)` proceeds in the
/// background and completion shows up as writability, after which
/// `SO_ERROR` tells success from failure.
pub async fn connect_tcp(addr: SocketAddr) -> io::Result<AsyncFd<TcpStream>> {
    let socket = AsyncFd::new(os::start_connect(addr)?)?;
    socket.writable().await;
    match socket.get_ref().take_error()? {
        Some(err) => Err(err),
        None => Ok(socket),
    }
}

/// Connect to a Unix stream socket, by path or abstract name. Like
/// socket-proxyd, a full listen backlog is an error rather than a reason to
/// wait.
pub fn connect_unix(addr: &UnixSocketAddr) -> io::Result<AsyncFd<UnixStream>> {
    AsyncFd::new(os::connect_unix(addr)?)
}
