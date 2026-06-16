//! The `sd_notify(3)` protocol: telling the service manager about start-up,
//! reloads, status and timeouts, and handing it file descriptors to keep.

use std::env;
use std::ffi::OsStr;
use std::io::{self, ErrorKind};
use std::os::fd::{AsFd, BorrowedFd};
use std::os::linux::net::SocketAddrExt;
use std::os::raw::c_int;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::net::{SocketAddr, UnixDatagram};
use std::path::Path;
use std::time::Duration;

use crate::os::{self, VsockAddr};

/// Send `state`, a newline-separated list of `KEY=VALUE` assignments, to
/// the service manager. Returns `false` if the service was not started
/// with a notification socket.
pub fn notify(state: &str) -> io::Result<bool> {
    notify_with_fds(state, &[])
}

/// Like [`notify`], passing `fds` along (`SCM_RIGHTS`), as the fd store
/// needs. Only possible over `AF_UNIX`.
pub fn notify_with_fds(state: &str, fds: &[BorrowedFd<'_>]) -> io::Result<bool> {
    let Some(target) = env::var_os("NOTIFY_SOCKET") else {
        return Ok(false);
    };
    match Target::parse(target.as_bytes())? {
        Target::Unix(address) => {
            let socket = UnixDatagram::unbound()?;
            socket.connect_addr(&address)?;
            os::send_with_fds(socket.as_fd(), state.as_bytes(), fds)?;
        }
        Target::Vsock { .. } if !fds.is_empty() => {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "AF_VSOCK can't carry file descriptors",
            ));
        }
        Target::Vsock { address, kind } => send_vsock(address, kind, state.as_bytes())?,
    }
    Ok(true)
}

/// Start-up or a reload finished; `status` is shown by `systemctl status`.
pub fn notify_ready(status: &str) -> io::Result<bool> {
    notify(&format!("READY=1\nSTATUS={status}"))
}

/// A reload started. `Type=notify-reload` matches the reload to the
/// `SIGHUP` it sent through the timestamp. Must be followed by
/// [`notify_ready`] once the reload is done, whether or not it worked.
pub fn notify_reloading() -> io::Result<bool> {
    notify(&format!(
        "RELOADING=1\nMONOTONIC_USEC={}",
        os::monotonic_usec()
    ))
}

/// Shutdown started. Final: the manager won't take `READY=1` after this.
pub fn notify_stopping(status: &str) -> io::Result<bool> {
    notify(&format!("STOPPING=1\nSTATUS={status}"))
}

pub fn notify_status(status: &str) -> io::Result<bool> {
    notify(&format!("STATUS={status}"))
}

/// The current phase (start-up, runtime or shutdown) needs `more` time
/// than its configured timeout allows. Send again before it runs out to
/// keep extending.
pub fn notify_extend_timeout(more: Duration) -> io::Result<bool> {
    notify(&format!("EXTEND_TIMEOUT_USEC={}", more.as_micros()))
}

/// Start-up failed with `err`, so that `systemctl status` shows why.
pub fn notify_failed(err: &io::Error) -> io::Result<bool> {
    let errno = err.raw_os_error().unwrap_or(match err.kind() {
        ErrorKind::NotFound => libc::ENOENT,
        ErrorKind::PermissionDenied => libc::EACCES,
        ErrorKind::InvalidInput | ErrorKind::InvalidData => libc::EINVAL,
        _ => libc::EIO,
    });
    notify(&format!("STATUS={err}\nERRNO={errno}"))
}

/// Keep `fd` in the service manager's fd store (`FileDescriptorStoreMax=`)
/// under `name`: it is passed back through [`listen_fds`](super::listen_fds)
/// when the service is restarted, so for example a listening socket
/// survives restarts and crashes without losing queued connections.
pub fn store_fd(name: &str, fd: BorrowedFd<'_>) -> io::Result<bool> {
    let valid = !name.is_empty()
        && name.len() <= 255
        && !name.contains(':')
        && !name.contains(char::is_control);
    if !valid {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            format!("invalid fd name {name:?}"),
        ));
    }
    notify_with_fds(&format!("FDSTORE=1\nFDNAME={name}"), &[fd])
}

/// Where `$NOTIFY_SOCKET` points.
#[derive(Debug)]
enum Target {
    Unix(SocketAddr),
    /// `kind` is the socket type, if the address names one.
    Vsock {
        address: VsockAddr,
        kind: Option<c_int>,
    },
}

impl Target {
    /// `/path`, `@abstract`, or `vsock[-dgram|-seqpacket|-stream]:CID:PORT`
    /// with a numeric CID or `hypervisor`, `local` or `host`.
    fn parse(s: &[u8]) -> io::Result<Self> {
        let invalid = || io::Error::new(ErrorKind::InvalidInput, "invalid NOTIFY_SOCKET");
        match s.first() {
            Some(b'@') => return Ok(Self::Unix(SocketAddr::from_abstract_name(&s[1..])?)),
            Some(b'/') => {
                return Ok(Self::Unix(SocketAddr::from_pathname(Path::new(
                    OsStr::from_bytes(s),
                ))?));
            }
            _ => {}
        }
        let s = std::str::from_utf8(s).map_err(|_| invalid())?;
        let (kind, address) = [
            ("vsock:", None),
            ("vsock-dgram:", Some(libc::SOCK_DGRAM)),
            ("vsock-seqpacket:", Some(libc::SOCK_SEQPACKET)),
            ("vsock-stream:", Some(libc::SOCK_STREAM)),
        ]
        .into_iter()
        .find_map(|(prefix, kind)| Some((kind, s.strip_prefix(prefix)?)))
        .ok_or_else(|| io::Error::from_raw_os_error(libc::EAFNOSUPPORT))?;
        let (cid, port) = address.split_once(':').ok_or_else(invalid)?;
        let cid = match cid {
            "hypervisor" => libc::VMADDR_CID_HYPERVISOR,
            "local" => libc::VMADDR_CID_LOCAL,
            "host" => libc::VMADDR_CID_HOST,
            // A notification needs a specific peer.
            "" | "any" | "-1" => return Err(invalid()),
            n => n.parse().map_err(|_| invalid())?,
        };
        let port = port
            .parse()
            .ok()
            .filter(|&p| p != libc::VMADDR_PORT_ANY)
            .ok_or_else(invalid)?;
        Ok(Self::Vsock {
            address: VsockAddr { cid, port },
            kind,
        })
    }
}

/// Without a type in the address, try datagrams, then sequential packets,
/// then streams: transports differ in which they offer (the loopback one
/// has no datagrams), and in how they refuse the others. Reports the last
/// type's error if none works.
fn send_vsock(address: VsockAddr, kind: Option<c_int>, data: &[u8]) -> io::Result<()> {
    const ANY_KIND: &[c_int] = &[libc::SOCK_DGRAM, libc::SOCK_SEQPACKET, libc::SOCK_STREAM];
    let mut result = Ok(());
    for &kind in kind.as_ref().map_or(ANY_KIND, std::slice::from_ref) {
        result = os::vsock_send(address, kind, data);
        if result.is_ok() {
            break;
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::Target;

    fn vsock(s: &str) -> Option<(u32, u32, Option<i32>)> {
        match Target::parse(s.as_bytes()) {
            Ok(Target::Vsock { address, kind }) => Some((address.cid, address.port, kind)),
            _ => None,
        }
    }

    #[test]
    fn parses_unix_addresses() {
        let path = Target::parse(b"/run/systemd/notify").unwrap();
        assert!(matches!(path, Target::Unix(a) if a.as_pathname().is_some()));
        let abstract_ = Target::parse(b"@notify").unwrap();
        assert!(matches!(abstract_, Target::Unix(a) if a.as_pathname().is_none()));
    }

    #[test]
    fn parses_vsock_addresses() {
        assert_eq!(vsock("vsock:2:1234"), Some((2, 1234, None)));
        assert_eq!(
            vsock("vsock-dgram:3:1"),
            Some((3, 1, Some(libc::SOCK_DGRAM)))
        );
        assert_eq!(
            vsock("vsock-seqpacket:local:9"),
            Some((1, 9, Some(libc::SOCK_SEQPACKET)))
        );
        assert_eq!(
            vsock("vsock-stream:host:0"),
            Some((2, 0, Some(libc::SOCK_STREAM)))
        );
        assert_eq!(vsock("vsock:hypervisor:7"), Some((0, 7, None)));
    }

    #[test]
    fn rejects_bad_addresses() {
        for bad in [
            "vsock::1",
            "vsock:any:1",
            "vsock:-1:1",
            "vsock:2",
            "vsock:2:4294967295",
            "vsock:x:1",
            "tcp:1:2",
            "",
        ] {
            assert!(Target::parse(bad.as_bytes()).is_err(), "{bad:?} accepted");
        }
    }
}
