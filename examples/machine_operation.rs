// SPDX-License-Identifier: LGPL-2.1-or-later

//! machined's `Operation`: long-running work forked off a D-Bus method
//! call, with [`Child`].
//!
//! Removing, cloning or resizing a machine image can take minutes, so
//! machined forks a child for it and answers the D-Bus call when the child
//! exits. The child reports failure as an errno through a pipe. In C that
//! takes an `Operation` object holding the child, the errno pipe, the
//! pending D-Bus message, an sd-event child source, an optional `done`
//! callback and two linked-list memberships:
//!
//! - method: https://github.com/systemd/systemd/blob/f1d45e3d1466b24ceed38e3a54289ecf31edf9d1/src/machine/image-dbus.c#L27-L85
//! - completion: https://github.com/systemd/systemd/blob/f1d45e3d1466b24ceed38e3a54289ecf31edf9d1/src/machine/operation.c#L20-L98
//! - bookkeeping: https://github.com/systemd/systemd/blob/f1d45e3d1466b24ceed38e3a54289ecf31edf9d1/src/machine/operation.c#L100-L165
//! - the struct: https://github.com/systemd/systemd/blob/f1d45e3d1466b24ceed38e3a54289ecf31edf9d1/src/machine/operation.h
//!
//! homed (`systemd-homework`) and udevd (per-event workers) use the same
//! fork-and-wait-for-exit shape.
//!
//! Here the method is an `async fn` and the D-Bus reply is its return
//! value. The pending message, the child source and the `done` callback
//! disappear into the future. Cancelling it, because the caller went away
//! or machined is shutting down, is dropping it, which kills and reaps the
//! child exactly like `operation_free()`'s `pidref_done_sigkill_wait()`.
//!
//! Verdict: the clearest win of all the ports. The C code splits one
//! linear story (check limit, fork, wait, read errno, reply) over four
//! functions and a struct whose fields exist only to carry state between
//! callbacks.
//!
//! ```sh
//! cargo run --example machine_operation
//! ```
//!
//! removes an image, fails to remove a missing one, hits the operation
//! limit, and gives up on an operation stuck on a dead disk.
//!
//! Links to the C code are pinned to systemd commit f1d45e3d.

#![forbid(unsafe_code)]

use std::cell::Cell;
use std::fs::File;
use std::io::{self, ErrorKind, Read, Write};
use std::os::fd::AsFd;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::rc::Rc;
use std::time::{Duration, Instant};

use sd_event_futures::executor::{block_on, spawn};
use sd_event_futures::os;
use sd_event_futures::process::Child;
use sd_event_futures::reactor::{sleep, yield_now};
use sd_event_futures::{log, select};

pub fn main() -> ExitCode {
    match block_on(run()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            log!(Error, "{e}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> io::Result<()> {
    let images = std::env::temp_dir().join(format!("machine-operation-{}", std::process::id()));
    let manager = Rc::new(Manager::new(1));

    let image = sample_image(&images.join("fedora"))?;
    let started = Instant::now();
    remove_image(&manager, image.clone()).await?;
    println!("Removed {} in {:.1?}.", image.display(), started.elapsed());

    let missing = images.join("missing");
    if let Err(e) = remove_image(&manager, missing.clone()).await {
        println!("Removing {} failed in the child: {e}", missing.display());
    }

    let busy = spawn({
        let manager = manager.clone();
        async move { manager.run_operation(stuck_disk).await }
    });
    yield_now().await;
    if let Err(e) = remove_image(&manager, images.join("other")).await {
        println!("A second operation is refused: {e}");
    }
    busy.abort();

    select! {
        _ = manager.run_operation(stuck_disk) => {},
        () = sleep(Duration::from_millis(100)) => println!(
            "Gave up after 100ms; the child is killed and reaped, {} operations remain.",
            manager.n_operations()
        ),
    }
    std::fs::remove_dir_all(&images)
}

pub struct Manager {
    n_operations: Cell<usize>,
    operations_max: usize,
}

impl Manager {
    pub fn new(operations_max: usize) -> Self {
        Self {
            n_operations: Cell::new(0),
            operations_max,
        }
    }

    pub fn n_operations(&self) -> usize {
        self.n_operations.get()
    }

    /// Run `work` in a child process and wait for it, without blocking
    /// the loop. A failure in the child comes back as its errno.
    pub async fn run_operation(&self, work: impl FnOnce() -> io::Result<()>) -> io::Result<()> {
        let _slot = OperationSlot::acquire(self)?;

        let (errno_rx, errno_tx) = os::pipe()?;
        let errno_tx = File::from(errno_tx);
        let mut child = Child::fork(&[errno_tx.as_fd()], || match work() {
            Ok(()) => 0,
            Err(e) => {
                // report_errno_and_exit()
                let errno = e.raw_os_error().unwrap_or(libc::EIO);
                let _ = (&errno_tx).write_all(&errno.to_ne_bytes());
                1
            }
        })?;
        drop(errno_tx);

        if child.wait().await?.success() {
            return Ok(());
        }
        // The child has exited, so this read doesn't block.
        let mut errno = [0; 4];
        match File::from(errno_rx).read_exact(&mut errno) {
            Ok(()) => Err(io::Error::from_raw_os_error(i32::from_ne_bytes(errno))),
            Err(_) => Err(io::Error::other("Child died abnormally")),
        }
    }
}

/// Counts as one ongoing operation for as long as it lives.
struct OperationSlot<'a>(&'a Manager);

impl<'a> OperationSlot<'a> {
    fn acquire(manager: &'a Manager) -> io::Result<Self> {
        let n = manager.n_operations.get();
        if n >= manager.operations_max {
            return Err(io::Error::new(
                ErrorKind::QuotaExceeded,
                "Too many ongoing operations.",
            ));
        }
        manager.n_operations.set(n + 1);
        Ok(Self(manager))
    }
}

impl Drop for OperationSlot<'_> {
    fn drop(&mut self) {
        self.0.n_operations.set(self.0.n_operations.get() - 1);
    }
}

/// `org.freedesktop.machine1.Image.Remove()`.
pub async fn remove_image(m: &Manager, image: PathBuf) -> io::Result<()> {
    m.run_operation(move || std::fs::remove_dir_all(&image))
        .await
}

// =============================================================================
// Scaffolding for the demonstration
// =============================================================================

/// A small OS tree at `dir`.
fn sample_image(dir: &Path) -> io::Result<PathBuf> {
    for d in ["usr/lib", "usr/bin", "etc"] {
        std::fs::create_dir_all(dir.join(d))?;
    }
    std::fs::write(dir.join("usr/lib/os-release"), b"ID=fedora\n")?;
    for n in 0..1000 {
        std::fs::write(dir.join(format!("usr/bin/tool{n}")), b"#!/bin/sh\n")?;
    }
    Ok(dir.to_owned())
}

/// Work that never finishes, like I/O on a disk that stopped answering.
pub fn stuck_disk() -> io::Result<()> {
    println!(
        "Child {} waits for a disk that stopped answering.",
        std::process::id()
    );
    std::thread::sleep(Duration::MAX);
    Ok(())
}
