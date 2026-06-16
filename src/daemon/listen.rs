//! Sockets passed in by socket activation and the fd store
//! (`sd_listen_fds(3)`).

use std::env;
use std::os::fd::{AsFd, FromRawFd, OwnedFd};
use std::sync::atomic::{AtomicBool, Ordering};

use crate::os;

/// The first descriptor the service manager passes, `SD_LISTEN_FDS_START`.
const FIRST: i32 = 3;

/// A socket passed in by socket activation or from the fd store.
#[derive(Debug)]
pub struct ListenFd {
    pub fd: OwnedFd,
    /// From `FileDescriptorName=` in the socket unit, or the name it was
    /// stored under with [`store_fd`](super::store_fd).
    pub name: Option<String>,
}

/// Take the sockets the service manager passed in. The first call takes
/// them all; later calls get none.
///
/// The `LISTEN_*` variables stay in the environment, like
/// `sd_listen_fds(0)` leaves them: they name this process, so child
/// processes ignore them.
pub fn listen_fds() -> Vec<ListenFd> {
    static TAKEN: AtomicBool = AtomicBool::new(false);
    if TAKEN.swap(true, Ordering::SeqCst) {
        return Vec::new();
    }
    let var = |key| env::var(key).ok();
    if !names_this_process(var("LISTEN_PID"), var("LISTEN_PIDFDID")) {
        return Vec::new();
    }
    let Some(count) = var("LISTEN_FDS").and_then(|n| n.parse::<i32>().ok()) else {
        return Vec::new();
    };
    let names = var("LISTEN_FDNAMES").unwrap_or_default();
    let mut names = names.split(':').map(str::to_owned);
    (FIRST..FIRST + count)
        .map(|raw| {
            // SAFETY: the service manager passed these open descriptors to
            // this process alone, and the swap above lets only one caller
            // take ownership of them.
            let fd = unsafe { OwnedFd::from_raw_fd(raw) };
            let _ = os::set_cloexec(fd.as_fd());
            ListenFd {
                fd,
                name: names.next().filter(|n| !n.is_empty()),
            }
        })
        .collect()
}

/// Whether `LISTEN_PID` and, if set, `LISTEN_PIDFDID` name this process.
/// The pidfd inode ID tells it apart from an earlier process that had the
/// same pid.
fn names_this_process(pid: Option<String>, pidfd_id: Option<String>) -> bool {
    pid.is_some_and(|pid| pid.parse() == Ok(std::process::id()))
        && pidfd_id.is_none_or(|id| os::own_pidfd_id().is_ok_and(|own| id.parse() == Ok(own)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn own_pid() -> Option<String> {
        Some(std::process::id().to_string())
    }

    fn own_pidfd_id() -> u64 {
        os::own_pidfd_id().unwrap()
    }

    #[test]
    fn needs_this_pid() {
        assert!(names_this_process(own_pid(), None));
        assert!(!names_this_process(None, None));
        assert!(!names_this_process(Some("1".into()), None));
        assert!(!names_this_process(Some("self".into()), None));
    }

    #[test]
    fn needs_this_pidfd_id_when_given() {
        let id = own_pidfd_id();
        assert!(names_this_process(own_pid(), Some(id.to_string())));
        assert!(!names_this_process(own_pid(), Some((id + 1).to_string())));
        assert!(!names_this_process(own_pid(), Some(String::new())));
        assert!(!names_this_process(Some("1".into()), Some(id.to_string())));
    }
}
