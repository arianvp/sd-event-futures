// SPDX-License-Identifier: LGPL-2.1-or-later

//! journald: taking a journal file offline without stalling logging.
//!
//! Marking a journal file offline means `fsync()`, writing the header
//! state, and `fsync()` again, which can stall for a long time on slow
//! storage. journald does it on a separate thread so it keeps accepting
//! log messages. The thread and the main loop coordinate through an
//! 8-state machine driven by compare-and-swap loops, so that a new offline
//! request can restart one in flight, and a later `set_online()` joins the
//! thread.
//!
//! - state machine and thread:
//!   https://github.com/systemd/systemd/blob/f1d45e3d1466b24ceed38e3a54289ecf31edf9d1/src/shared/journal-file-util.c#L147-L327
//! - journal_file_set_offline():
//!   https://github.com/systemd/systemd/blob/f1d45e3d1466b24ceed38e3a54289ecf31edf9d1/src/shared/journal-file-util.c#L339-L408
//!
//! Here the offline in flight is a [`JoinHandle`]. A new request waits for
//! it and starts another instead of restarting it midway, which costs at
//! most one extra `fsync()` and removes the state machine.
//!
//! ```sh
//! cargo run --example journal_offline [DIRECTORY]
//! ```
//!
//! writes 64 MiB of entries to a journal file in `DIRECTORY` (the current
//! one by default; not a tmpfs, so that syncing takes a while), takes it
//! offline, and keeps logging while that syncs.
//!
//! Links to the C code are pinned to systemd commit f1d45e3d.

#![forbid(unsafe_code)]

use std::fs::File;
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::{Duration, Instant};

use sd_event_futures::executor::{JoinHandle, block_on, spawn, spawn_blocking};
use sd_event_futures::log;
use sd_event_futures::reactor::sleep;

/// The header's `state` field: `STATE_OFFLINE` and `STATE_ONLINE`.
pub const OFFLINE: u8 = 0;
pub const ONLINE: u8 = 1;

pub fn main() -> ExitCode {
    let dir = std::env::args_os()
        .nth(1)
        .map_or_else(|| ".".into(), PathBuf::from);
    match block_on(run(&dir.join("offline-example.journal"))) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            log!(Error, "{e}");
            ExitCode::FAILURE
        }
    }
}

async fn run(path: &Path) -> io::Result<()> {
    let mut journal = JournalFile::create(path)?;
    let entry = [b'x'; 4096];
    for _ in 0..(64 << 20) / entry.len() {
        journal.append(&entry)?;
    }

    let started = Instant::now();
    journal.set_offline().await?;
    let mut logged = 0;
    while journal.is_offlining() {
        journal.append(b"MESSAGE=logged while going offline\n")?;
        logged += 1;
        sleep(Duration::from_millis(1)).await;
    }
    journal.set_online().await?;
    println!(
        "Took the journal offline in {:.1?}, and logged {logged} messages meanwhile.",
        started.elapsed()
    );
    std::fs::remove_file(path)
}

/// A journal file, reduced to its header's state and appended entries.
pub struct JournalFile {
    file: Arc<File>,
    offlining: Option<JoinHandle<io::Result<()>>>,
}

impl JournalFile {
    pub fn create(path: &Path) -> io::Result<Self> {
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        file.write_all_at(&[ONLINE], 0)?;
        Ok(Self {
            file: Arc::new(file),
            offlining: None,
        })
    }

    pub fn append(&self, entry: &[u8]) -> io::Result<()> {
        let end = self.file.metadata()?.len();
        self.file.write_all_at(entry, end)
    }

    /// `journal_file_set_offline(f, /* wait= */ false)`: start taking the
    /// file offline, after any offline still in flight.
    pub async fn set_offline(&mut self) -> io::Result<()> {
        self.join_offline().await?;
        let file = self.file.clone();
        self.offlining = Some(spawn(spawn_blocking(move || {
            file.sync_all()?;
            file.write_all_at(&[OFFLINE], 0)?;
            file.sync_all()
        })));
        Ok(())
    }

    pub fn is_offlining(&self) -> bool {
        self.offlining.as_ref().is_some_and(|h| !h.is_finished())
    }

    /// `journal_file_set_online()`: an offline in flight must finish first.
    pub async fn set_online(&mut self) -> io::Result<()> {
        self.join_offline().await?;
        self.file.write_all_at(&[ONLINE], 0)
    }

    pub fn state(&self) -> io::Result<u8> {
        let mut state = [0];
        self.file.read_exact_at(&mut state, 0)?;
        Ok(state[0])
    }

    async fn join_offline(&mut self) -> io::Result<()> {
        match self.offlining.take() {
            Some(offlining) => offlining.await,
            None => Ok(()),
        }
    }
}
