//! Logging to stderr the way journald reads it.

use std::fmt;
use std::io::{self, Write};
use std::os::fd::AsFd;
use std::sync::OnceLock;

use crate::os;

/// Syslog priorities, as used by the `<N>` prefixes journald understands.
#[derive(Copy, Clone, Debug)]
pub enum Priority {
    Error = 3,
    Warning = 4,
    Notice = 5,
    Info = 6,
    Debug = 7,
}

/// [`log`] with `format!` arguments, and the [`Priority`] by name:
/// `log!(Warning, "Failed to accept connection: {e}")`.
#[macro_export]
macro_rules! log {
    ($priority:ident, $($arg:tt)*) => {
        $crate::daemon::log(
            $crate::daemon::Priority::$priority,
            ::core::format_args!($($arg)*),
        )
    };
}

/// Log a line to stderr, prefixed with its priority if stderr is
/// connected to the journal (`$JOURNAL_STREAM`), and plain otherwise.
pub fn log(priority: Priority, message: fmt::Arguments<'_>) {
    let line = if stderr_is_journal() {
        format!("<{}>{message}\n", priority as u8)
    } else {
        format!("{message}\n")
    };
    let _ = io::stderr().write_all(line.as_bytes());
}

/// Whether stderr is the stream `$JOURNAL_STREAM` names, as `device:inode`.
fn stderr_is_journal() -> bool {
    static IS_JOURNAL: OnceLock<bool> = OnceLock::new();
    *IS_JOURNAL.get_or_init(|| {
        let stream = std::env::var("JOURNAL_STREAM").ok();
        let stderr = os::file_identity(io::stderr().as_fd()).ok();
        stream
            .zip(stderr)
            .is_some_and(|(stream, (device, inode))| stream == format!("{device}:{inode}"))
    })
}
