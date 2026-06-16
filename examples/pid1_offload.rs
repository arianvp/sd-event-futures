// SPDX-License-Identifier: LGPL-2.1-or-later

//! PID 1's offloading, with [`Child`].
//!
//! PID 1 has no threads. Work that might block it, such as `sync()`,
//! `close()` or removing a directory tree, runs in a forked helper process
//! instead. `src/shared/async.c` gives the reason: "a thread would do that
//! as the process can't exit with threads hanging in blocking syscalls".
//!
//! ```sh
//! cargo run --example pid1_offload [DIRECTORY]
//! ```
//!
//! In `DIRECTORY` (the current one by default; not a tmpfs, so that there
//! is something to sync) it removes a tree of 10 000 files, closes a pipe,
//! and writes 128 MiB that it then syncs the way shutdown does, printing
//! the dirty memory left as it goes. A task ticks every millisecond
//! meanwhile, to show that the loop never stops.
//!
//! Like PID 1, this program is single-threaded, which [`Child::fork`]
//! checks: the one condition under which a forked child may do anything.
//! The C code's `pidref_safe_fork()` assumes it instead.
//!
//! Links to the C code are pinned to systemd commit f1d45e3d.

#![forbid(unsafe_code)]

use std::cell::Cell;
use std::fmt;
use std::fs::File;
use std::io::{self, ErrorKind, Read, Write};
use std::os::fd::{AsRawFd, OwnedFd};
use std::path::{Path, PathBuf};
use std::process::{ExitCode, ExitStatus};
use std::rc::Rc;
use std::time::{Duration, Instant};

use sd_event_futures::executor::{block_on, spawn};
use sd_event_futures::os;
use sd_event_futures::process::Child;
use sd_event_futures::reactor::sleep;
use sd_event_futures::{log, select};

pub fn main() -> ExitCode {
    let dir = std::env::args_os()
        .nth(1)
        .map_or_else(|| ".".into(), PathBuf::from);
    match block_on(run(&dir)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            log!(Error, "{e}");
            ExitCode::FAILURE
        }
    }
}

async fn run(dir: &Path) -> io::Result<()> {
    let ticks = Ticks::start();

    let runtime_dir = dir.join("pid1-offload-runtime");
    create_tree(&runtime_dir, 100, 100)?;
    let watch = ticks.stopwatch();
    exited_ok(asynchronous_rm_rf(runtime_dir)?.wait().await?)?;
    println!("Removed a runtime directory of 10000 files {watch}.");

    let (read_end, write_end) = os::pipe()?;
    let fd = write_end.as_raw_fd();
    let mut closer = asynchronous_close(write_end)?;
    exited_ok(closer.wait().await?)?;
    let eof = File::from(read_end).read(&mut [0])? == 0;
    println!(
        "Closed fd {fd} in helper process {}; the pipe's read end {} end-of-file.",
        closer.id(),
        if eof { "sees" } else { "does not see" }
    );

    let data = dir.join("pid1-offload.data");
    write_unsynced(&data, 128 << 20)?;
    println!("Wrote 128 MiB without syncing: {} kB dirty.", dirty_kb()?);
    let watch = ticks.stopwatch();
    let progress = meminfo_progress(|kb| println!("  still {kb} kB dirty"));
    sync_with_progress(asynchronous_sync()?, progress, STEP, ATTEMPTS).await?;
    println!("Synced {watch}: {} kB dirty.", dirty_kb()?);
    std::fs::remove_file(&data)
}

// =============================================================================
// asynchronous_rm_rf()
// =============================================================================
//
// When a unit stops, execute.c removes its runtime directory in the
// background, best effort.
//
// - https://github.com/systemd/systemd/blob/f1d45e3d1466b24ceed38e3a54289ecf31edf9d1/src/shared/async.c#L143-L170
// - caller: https://github.com/systemd/systemd/blob/f1d45e3d1466b24ceed38e3a54289ecf31edf9d1/src/core/execute.c#L2467
//
// Verdict: the same as C, with `std::fs::remove_dir_all` for `rm_rf()`,
// and the child is owned: waited for, detached on purpose, or killed and
// reaped when dropped.

pub fn asynchronous_rm_rf(path: PathBuf) -> io::Result<Child> {
    Child::fork(&[], move || {
        i32::from(std::fs::remove_dir_all(&path).is_err())
    })
}

// =============================================================================
// asynchronous_close()
// =============================================================================
//
// `close()` can block, for example on the last reference to a file on a
// stuck network filesystem. PID 1 closes service fds (stdio, sockets,
// root directory fds) from a child that shares its fd table through
// `CLONE_FILES`, so the descriptor goes away in PID 1 too.
//
// - https://github.com/systemd/systemd/blob/f1d45e3d1466b24ceed38e3a54289ecf31edf9d1/src/shared/async.c#L59-L134
// - callers: https://github.com/systemd/systemd/blob/f1d45e3d1466b24ceed38e3a54289ecf31edf9d1/src/core/service.c#L586-L588
//
// Verdict: better than C. The C function returns -EBADF so callers can
// write `s->fd = asynchronous_close(s->fd)` and must not touch the old
// number again; `Child::close()` takes the `OwnedFd`, so using it
// afterwards doesn't compile. The `clone3()` call lives behind that safe
// function, and the C version's double fork for callers that don't reap
// is unnecessary because `Child` reaps the helper itself.

pub fn asynchronous_close(fd: OwnedFd) -> io::Result<Child> {
    Child::close(fd)
}

// =============================================================================
// asynchronous_sync() and shutdown's sync_with_progress()
// =============================================================================
//
// job.c starts a `sync()` when a shutdown job begins, without waiting for
// it. The final shutdown syncs from a helper too, and watches it: as long
// as the amount of dirty memory keeps shrinking it waits in
// SYNC_TIMEOUT_USEC steps, and after SYNC_PROGRESS_ATTEMPTS steps without
// progress it gives up and kills the helper.
//
// - https://github.com/systemd/systemd/blob/f1d45e3d1466b24ceed38e3a54289ecf31edf9d1/src/shared/async.c#L15-L56
// - https://github.com/systemd/systemd/blob/f1d45e3d1466b24ceed38e3a54289ecf31edf9d1/src/shutdown/shutdown.c#L190-L285
//
// Verdict: clearer than C. "Wait for the child, but at most one step" is a
// `select!` with a timer instead of a separate
// `pidref_wait_for_terminate_full()` call with a timeout argument, and
// the loop says exactly what it does. The C code is fine too; it is a
// synchronous program, so there is less to gain here than in a daemon.

/// shutdown.c waits 10 s per step; short enough here to watch.
const STEP: Duration = Duration::from_millis(100);
const ATTEMPTS: u32 = 3;

pub fn asynchronous_sync() -> io::Result<Child> {
    Child::fork(&[], || {
        os::sync();
        0
    })
}

/// Wait for `sync` to finish, for as long as `making_progress` says it
/// does, checking every `step`.
pub async fn sync_with_progress(
    mut sync: Child,
    mut making_progress: impl FnMut() -> bool,
    step: Duration,
    attempts: u32,
) -> io::Result<()> {
    let mut checks = 0;
    while checks < attempts {
        select! {
            status = sync.wait() => {
                let status = status?;
                return if status.success() {
                    Ok(())
                } else {
                    Err(io::Error::other(format!("sync helper failed: {status}")))
                };
            },
            () = sleep(step) => checks = if making_progress() { 0 } else { checks + 1 },
        }
    }
    let _ = sync.kill(libc::SIGKILL);
    // PID 1 reaps it eventually; shutdown doesn't wait for a stuck sync.
    sync.detach();
    Err(io::Error::new(ErrorKind::TimedOut, "sync timed out"))
}

/// `sync_making_progress()`: whether dirty memory shrank since the last
/// call. Each reading also goes to `report`.
pub fn meminfo_progress(report: impl Fn(u64)) -> impl FnMut() -> bool {
    let mut last = u64::MAX;
    move || {
        let Ok(now) = dirty_kb() else { return false };
        report(now);
        let shrank = now < last;
        last = now;
        shrank
    }
}

/// Sum of the `Dirty`, `Writeback` and `NFS_Unstable` lines of
/// `/proc/meminfo`, in kB.
fn dirty_kb() -> io::Result<u64> {
    let meminfo = std::fs::read_to_string("/proc/meminfo")?;
    let mut total = 0;
    for line in meminfo.lines() {
        let mut words = line.split_whitespace();
        if let (Some("Dirty:" | "Writeback:" | "NFS_Unstable:"), Some(kb)) =
            (words.next(), words.next())
        {
            total += kb
                .parse::<u64>()
                .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?;
        }
    }
    Ok(total)
}

// =============================================================================
// Scaffolding for the demonstration
// =============================================================================

/// Counts the turns of a task that wakes every millisecond: proof that
/// the loop keeps running.
struct Ticks(Rc<Cell<u64>>);

impl Ticks {
    fn start() -> Self {
        let ticks = Rc::new(Cell::new(0));
        let counter = ticks.clone();
        spawn(async move {
            loop {
                sleep(Duration::from_millis(1)).await;
                counter.set(counter.get() + 1);
            }
        });
        Self(ticks)
    }

    fn stopwatch(&self) -> Stopwatch {
        Stopwatch {
            started: Instant::now(),
            ticks: self.0.clone(),
            ticks_then: self.0.get(),
        }
    }
}

/// Displays as "in 12.3ms, while the loop ticked 12 times".
struct Stopwatch {
    started: Instant,
    ticks: Rc<Cell<u64>>,
    ticks_then: u64,
}

impl fmt::Display for Stopwatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "in {:.1?}, while the loop ticked {} times",
            self.started.elapsed(),
            self.ticks.get() - self.ticks_then
        )
    }
}

fn exited_ok(status: ExitStatus) -> io::Result<()> {
    if status.success() {
        Ok(())
    } else {
        Err(io::Error::other(format!("helper failed: {status}")))
    }
}

/// `dirs` directories of `files` small files each under `root`.
fn create_tree(root: &Path, dirs: usize, files: usize) -> io::Result<()> {
    for d in 0..dirs {
        let dir = root.join(format!("{d}"));
        std::fs::create_dir_all(&dir)?;
        for f in 0..files {
            std::fs::write(dir.join(format!("{f}")), b"runtime state\n")?;
        }
    }
    Ok(())
}

/// `len` bytes in a new file at `path`, left in the page cache unsynced.
fn write_unsynced(path: &Path, len: usize) -> io::Result<()> {
    let chunk = vec![0x5a; 1 << 20];
    let mut file = File::create(path)?;
    for _ in 0..len / chunk.len() {
        file.write_all(&chunk)?;
    }
    Ok(())
}
