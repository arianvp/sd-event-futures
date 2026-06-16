//! Starting a program socket-activated, as systemd would.

use std::fs::File;
use std::io::{Cursor, Write};
use std::os::fd::{AsRawFd, BorrowedFd};
use std::os::unix::process::CommandExt;
use std::process::Command;

use sd_event_futures::os;

/// A command that starts `program` socket-activated: `fds` become fd 3,
/// 4, … and `LISTEN_FDS`, `LISTEN_FDNAMES`, `LISTEN_PID` and
/// `LISTEN_PIDFDID` describe them. Arguments added to the returned command
/// go to `program`; a `LISTEN_PIDFDID` set on it replaces the right one.
///
/// Like a service manager, it starts from a clean environment: variables
/// of the session running the tests (its own `MEMORY_PRESSURE_WRITE`,
/// say) must not leak into the service.
pub fn socket_activated(program: &str, fds: &[(BorrowedFd<'_>, &str)]) -> Command {
    // The pid and pidfd ID are the service's, which only exist after the
    // fork: the child writes its pidfd ID to a pipe, and a shell, which
    // then execs the service, reads it and sets the variables.
    let id_fd = 3 + fds.len();
    let script = format!(
        r#"read -r id <&{id_fd} &&
           export LISTEN_PID=$$ LISTEN_PIDFDID="${{LISTEN_PIDFDID:-$id}}" &&
           exec "$0" "$@" {id_fd}<&-"#
    );
    let mut command = Command::new("/bin/sh");
    command
        .env_clear()
        .args(["-c", &script, program])
        .env("LISTEN_FDS", fds.len().to_string())
        .env(
            "LISTEN_FDNAMES",
            fds.iter()
                .map(|(_, name)| *name)
                .collect::<Vec<_>>()
                .join(":"),
        );
    let (id_rd, id_wr) = os::pipe().unwrap();
    let id_wr = File::from(id_wr);
    let raw: Vec<i32> = fds
        .iter()
        .map(|(fd, _)| fd.as_raw_fd())
        .chain([id_rd.as_raw_fd()])
        .collect();
    // SAFETY: the hook only makes system calls and formats into a buffer
    // on its stack (renumber_fds() and own_pidfd_id() allocate nothing),
    // which is all that is allowed between fork and exec.
    #[allow(unsafe_code)]
    unsafe {
        command.pre_exec(move || {
            // Moved in to stay open until the fork.
            let _ = &id_rd;
            let mut line = Cursor::new([0u8; 21]);
            writeln!(line, "{}", os::own_pidfd_id()?)?;
            let len = line.position() as usize;
            // Before renumbering, which may reuse its number.
            (&id_wr).write_all(&line.get_ref()[..len])?;
            os::renumber_fds(&raw, 3)
        })
    };
    command
}
