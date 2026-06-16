//! The service side of systemd's daemon interfaces, without libsystemd's
//! `sd-daemon` API.
//!
//! These protocols are stable and documented for reimplementation:
//! `sd_notify(3)` (readiness, reload, status, timeouts and the fd store),
//! `sd_listen_fds(3)` (socket activation), `systemd.exec(5)` (credentials),
//! `sd-daemon(3)` (log priority prefixes) and `systemd.time(7)`. The
//! watchdog needs no code here: [`crate::Event::set_watchdog`] makes the
//! event loop send the keep-alive pings itself.

mod listen;
mod log;
mod notify;
mod time_span;

use std::path::{Path, PathBuf};

pub use listen::{ListenFd, listen_fds};
pub use log::{Priority, log};
pub use notify::{
    notify, notify_extend_timeout, notify_failed, notify_ready, notify_reloading, notify_status,
    notify_stopping, notify_with_fds, store_fd,
};
pub use time_span::parse_time_span;

/// Path of credential `name` (`LoadCredential=`, `SetCredential=`, …), if
/// the service manager passed one in.
pub fn credential(name: &str) -> Option<PathBuf> {
    let path = Path::new(&std::env::var_os("CREDENTIALS_DIRECTORY")?).join(name);
    path.exists().then_some(path)
}
