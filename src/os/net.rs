//! Name resolution through `getaddrinfo(3)`.

use std::ffi::{CStr, CString};
use std::io;
use std::net::SocketAddr;

use super::socket::to_socket_addr;

/// The first address `getaddrinfo(3)` finds for a stream connection to
/// `node` and `service`, which is a port number or a name from
/// `/etc/services`. Blocks, so call it from a blocking pool thread.
///
/// Failures carry the `gai_strerror(3)` text.
pub fn resolve(node: &str, service: &str) -> io::Result<SocketAddr> {
    let c = |s: &str| {
        CString::new(s)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "Name or service not known"))
    };
    let (node, service) = (c(node)?, c(service)?);
    // SAFETY: all zeroes is a valid `addrinfo` hint: no flags, any protocol.
    let mut hints: libc::addrinfo = unsafe { std::mem::zeroed() };
    hints.ai_family = libc::AF_UNSPEC;
    hints.ai_socktype = libc::SOCK_STREAM;

    let mut found: *mut libc::addrinfo = std::ptr::null_mut();
    // SAFETY: both strings are NUL-terminated, `found` receives the list.
    let r = unsafe { libc::getaddrinfo(node.as_ptr(), service.as_ptr(), &hints, &mut found) };
    if r != 0 {
        return Err(match r {
            libc::EAI_SYSTEM => io::Error::last_os_error(),
            // SAFETY: `gai_strerror()` returns a static string for any code.
            r => io::Error::other(
                unsafe { CStr::from_ptr(libc::gai_strerror(r)) }
                    .to_string_lossy()
                    .into_owned(),
            ),
        });
    }
    // SAFETY: on success the list has at least one entry, whose address is
    // `ai_addrlen` bytes and fits a `sockaddr_storage`.
    let first = unsafe {
        let entry = &*found;
        let mut storage: libc::sockaddr_storage = std::mem::zeroed();
        std::ptr::copy_nonoverlapping(
            entry.ai_addr.cast::<u8>(),
            (&raw mut storage).cast::<u8>(),
            entry.ai_addrlen as usize,
        );
        storage
    };
    // SAFETY: `found` came from `getaddrinfo()` and is freed once.
    unsafe { libc::freeaddrinfo(found) };
    to_socket_addr(&first)
        .ok_or_else(|| io::Error::other("Address family for hostname not supported"))
}
