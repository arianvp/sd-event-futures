//! Moving data between two sockets with `splice(2)`, like systemd's
//! `socket-forward.c`: bytes go from one socket into a kernel pipe and
//! from there into the other socket, never through user space.

use std::io;
use std::os::fd::AsFd;

use crate::combinator::join;
use crate::os;
use crate::reactor::AsyncFd;

/// The pipe size `socket-forward.c` asks for.
const PIPE_SIZE: usize = 256 * 1024;

/// Forward in both directions until each has ended, then return the first
/// error, if any. A direction ends when its source reaches end of file or
/// either side disconnects; its destination is then shut down for writing,
/// so the peer sees end of file too.
pub async fn forward<A: AsFd, B: AsFd>(a: &AsyncFd<A>, b: &AsyncFd<B>) -> io::Result<()> {
    let (a_to_b, b_to_a) = join(simplex(a, b), simplex(b, a)).await;
    a_to_b.and(b_to_a)
}

async fn simplex<F: AsFd, T: AsFd>(from: &AsyncFd<F>, to: &AsyncFd<T>) -> io::Result<()> {
    let result = shovel(from, to).await;
    // The peer may already be gone; there is nothing left to tell it then.
    let _ = os::shutdown_write(to.get_ref().as_fd());
    result
}

async fn shovel<F: AsFd, T: AsFd>(from: &AsyncFd<F>, to: &AsyncFd<T>) -> io::Result<()> {
    let (pipe_out, pipe_in) = os::nonblocking_pipe()?;
    let capacity = os::resize_pipe(pipe_in.as_fd(), PIPE_SIZE)?;
    loop {
        // The pipe is empty here and full below, so `WouldBlock` always
        // means the socket isn't ready, which is what `*_with` waits for.
        let read = from
            .read_with(|s| os::splice(s.as_fd(), pipe_in.as_fd(), capacity))
            .await;
        let Some(mut buffered) = ended(read)?.filter(|&n| n > 0) else {
            return Ok(());
        };
        while buffered > 0 {
            let written = to
                .write_with(|s| os::splice(pipe_out.as_fd(), s.as_fd(), buffered))
                .await;
            let Some(n) = ended(written)?.filter(|&n| n > 0) else {
                return Ok(());
            };
            buffered -= n;
        }
    }
}

/// Disconnects end a direction without being an error, as in
/// `socket-forward.c`.
fn ended(result: io::Result<usize>) -> io::Result<Option<usize>> {
    match result {
        Ok(n) => Ok(Some(n)),
        Err(e) if is_disconnect(&e) => Ok(None),
        Err(e) => Err(e),
    }
}

/// `ERRNO_IS_DISCONNECT()`.
pub fn is_disconnect(e: &io::Error) -> bool {
    matches!(
        e.raw_os_error(),
        Some(
            libc::ECONNABORTED
                | libc::ECONNREFUSED
                | libc::ECONNRESET
                | libc::EHOSTDOWN
                | libc::EHOSTUNREACH
                | libc::ENETDOWN
                | libc::ENETRESET
                | libc::ENETUNREACH
                | libc::ENONET
                | libc::ENOPROTOOPT
                | libc::ENOTCONN
                | libc::EPIPE
                | libc::EPROTO
                | libc::ESHUTDOWN
                | libc::ETIMEDOUT
        )
    )
}
