//! Tests of `examples/pid1_offload.rs`. Like PID 1, this test binary is
//! single-threaded (see `run_single_threaded`).
#![deny(unsafe_code)]

mod common;
#[allow(dead_code)]
#[path = "../examples/pid1_offload.rs"]
mod pid1_offload;

use std::fs::File;
use std::io::{ErrorKind, Read};
use std::os::unix::process::ExitStatusExt;
use std::time::{Duration, Instant};

use sd_event_futures::executor::block_on;
use sd_event_futures::os;
use sd_event_futures::process::Child;

use common::within;
use pid1_offload::{
    asynchronous_close, asynchronous_rm_rf, asynchronous_sync, meminfo_progress, sync_with_progress,
};

const LIMIT: Duration = Duration::from_secs(5);
const STEP: Duration = Duration::from_millis(20);

fn rm_rf_removes_the_tree() {
    let dir = std::env::temp_dir().join(format!("sd-event-futures-rmrf-{}", std::process::id()));
    std::fs::create_dir_all(dir.join("a/b/c")).unwrap();
    std::fs::write(dir.join("a/b/c/file"), b"x").unwrap();
    let status = block_on(async {
        let mut rm = asynchronous_rm_rf(dir.clone()).unwrap();
        within(LIMIT, rm.wait()).await.unwrap()
    });
    assert!(status.success());
    assert!(!dir.exists());
}

fn close_closes_here_too() {
    let (rd, wr) = os::pipe().unwrap();
    block_on(async {
        let mut close = asynchronous_close(wr).unwrap();
        assert!(within(LIMIT, close.wait()).await.unwrap().success());
    });
    // The write end is gone from this process too, so the reader sees EOF.
    let mut rest = Vec::new();
    File::from(rd).read_to_end(&mut rest).unwrap();
    assert!(rest.is_empty());
}

fn sync_finishes() {
    let progress = meminfo_progress(|_| {});
    block_on(within(LIMIT, async {
        sync_with_progress(asynchronous_sync().unwrap(), progress, STEP, 3).await
    }))
    .unwrap();
}

fn sync_without_progress_is_killed() {
    // A sync that never finishes, like one stuck on a dead NFS server.
    let stuck = Child::fork(&[], || {
        std::thread::sleep(Duration::MAX);
        0
    })
    .unwrap();
    let pid = stuck.id();
    let started = Instant::now();
    let err = block_on(within(LIMIT, sync_with_progress(stuck, || false, STEP, 3))).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::TimedOut);
    assert!(started.elapsed() >= STEP * 3);

    // Killed but left unreaped; collect it as PID 1 would.
    assert_eq!(os::waitpid(pid).unwrap().signal(), Some(libc::SIGKILL));
}

fn sync_making_progress_gets_more_time() {
    // Takes 8 steps, but reports progress every time, so 3 attempts suffice.
    let slow = Child::fork(&[], || {
        std::thread::sleep(STEP * 8);
        0
    })
    .unwrap();
    block_on(within(LIMIT, sync_with_progress(slow, || true, STEP, 3))).unwrap();
}

fn main() -> std::process::ExitCode {
    common::run_single_threaded(common::tests![
        rm_rf_removes_the_tree,
        close_closes_here_too,
        sync_finishes,
        sync_without_progress_is_killed,
        sync_making_progress_gets_more_time,
    ])
}
