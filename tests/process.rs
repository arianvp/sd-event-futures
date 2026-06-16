//! `process::Child` tests, run single-threaded (see `run_single_threaded`).
#![deny(unsafe_code)]

mod common;

use std::fs::File;
use std::io::{Read, Write};
use std::os::fd::AsFd;
use std::os::unix::process::ExitStatusExt;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use sd_event_futures::combinator::join_all;
use sd_event_futures::executor::{block_on, spawn_blocking};
use sd_event_futures::os;
use sd_event_futures::process::Child;
use sd_event_futures::reactor::sleep;

use common::within;

const LIMIT: Duration = Duration::from_secs(2);

/// A child that never exits by itself.
fn forever() -> i32 {
    std::thread::sleep(Duration::MAX);
    0
}

fn is_zombie(pid: libc::pid_t) -> bool {
    std::fs::read_to_string(format!("/proc/{pid}/stat")).is_ok_and(|stat| {
        stat.rsplit(')')
            .next()
            .unwrap()
            .trim_start()
            .starts_with('Z')
    })
}

fn wait_returns_exit_code() {
    let status = block_on(async {
        let mut child = Child::fork(&[], || 7).unwrap();
        within(LIMIT, child.wait()).await.unwrap()
    });
    assert_eq!(status.code(), Some(7));
}

fn wait_twice_returns_same_status() {
    block_on(async {
        let mut child = Child::fork(&[], || 3).unwrap();
        let first = within(LIMIT, child.wait()).await.unwrap();
        let second = within(LIMIT, child.wait()).await.unwrap();
        assert_eq!(first, second);
    });
}

fn killed_child_reports_signal() {
    // The parent may have signals blocked; the child starts with none.
    let status = block_on(async {
        let mut child = Child::fork(&[], forever).unwrap();
        child.kill(libc::SIGTERM).unwrap();
        within(LIMIT, child.wait()).await.unwrap()
    });
    assert_eq!(status.signal(), Some(libc::SIGTERM));
}

fn panic_in_child_exits_101() {
    let status = block_on(async {
        let mut child = Child::fork(&[], || {
            // Expected; keep it out of the test output.
            std::panic::set_hook(Box::new(|_| {}));
            panic!("child boom")
        })
        .unwrap();
        within(LIMIT, child.wait()).await.unwrap()
    });
    assert_eq!(status.code(), Some(101));
}

fn forking_refuses_with_threads_running() {
    let (tx, rx) = std::sync::mpsc::channel::<()>();
    let thread = std::thread::spawn(move || rx.recv());
    let err = Child::fork(&[], || 0)
        .err()
        .expect("forked with two threads");
    assert!(err.to_string().contains("2 threads"), "{err}");
    drop(tx);
    thread.join().unwrap().unwrap_err();
}

fn dropping_a_child_kills_and_reaps_it() {
    let child = Child::fork(&[], forever).unwrap();
    let pid = child.id();
    drop(child);
    assert!(!os::process_exists(pid), "child {pid} still exists");
}

fn dropping_a_child_inside_the_loop_reaps_it_in_the_background() {
    block_on(async {
        let child = Child::fork(&[], forever).unwrap();
        let pid = child.id();
        drop(child);
        // Killed, but the loop wasn't blocked waiting for it to go.
        assert!(os::process_exists(pid));
        let deadline = Instant::now() + LIMIT;
        while os::process_exists(pid) {
            assert!(Instant::now() < deadline, "child {pid} was never reaped");
            sleep(Duration::from_millis(1)).await;
        }
    });
}

fn detached_child_is_left_for_someone_else_to_reap() {
    let child = Child::fork(&[], || 0).unwrap();
    let pid = child.id();
    child.detach();
    let deadline = Instant::now() + LIMIT;
    while !is_zombie(pid) {
        assert!(Instant::now() < deadline, "child {pid} never exited");
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(os::waitpid(pid).unwrap().success());
}

fn fork_closes_fds_unless_kept() {
    let (_rd, wr) = os::pipe().unwrap();
    // Duplicating fails once the descriptor is closed.
    let still_open = || i32::from(wr.try_clone().is_err());
    block_on(async {
        let mut closed = Child::fork(&[], still_open).unwrap();
        assert_eq!(within(LIMIT, closed.wait()).await.unwrap().code(), Some(1));
        let mut kept = Child::fork(&[wr.as_fd()], still_open).unwrap();
        assert_eq!(within(LIMIT, kept.wait()).await.unwrap().code(), Some(0));
    });
}

fn kept_fds_work_in_the_child() {
    let (rd, wr) = os::pipe().unwrap();
    block_on(async {
        let mut child = Child::fork(&[wr.as_fd()], || {
            let mut out = File::from(wr.try_clone().unwrap());
            i32::from(out.write_all(b"from the child").is_err())
        })
        .unwrap();
        assert!(within(LIMIT, child.wait()).await.unwrap().success());
    });
    drop(wr);
    let mut message = String::new();
    File::from(rd).read_to_string(&mut message).unwrap();
    assert_eq!(message, "from the child");
}

fn close_in_child_closes_here_too() {
    let (rd, wr) = os::pipe().unwrap();
    block_on(async {
        let mut child = Child::close(wr).unwrap();
        assert!(within(LIMIT, child.wait()).await.unwrap().success());
    });
    // The write end is gone from this process too, so the reader sees EOF.
    let mut rest = Vec::new();
    File::from(rd).read_to_end(&mut rest).unwrap();
    assert!(rest.is_empty());
}

fn forks_once_blocking_jobs_are_done() {
    for _ in 0..2 {
        block_on(async {
            let jobs =
                (0..4).map(|_| spawn_blocking(|| std::thread::sleep(Duration::from_millis(20))));
            within(LIMIT, join_all(jobs)).await;
            assert!(os::thread_count() > 1, "no idle pool threads to retire");
            let mut child = Child::fork(&[], || 0).unwrap();
            assert!(within(LIMIT, child.wait()).await.unwrap().success());
        });
        assert_eq!(os::thread_count(), 1);
    }
}

fn forking_refuses_while_a_blocking_job_runs() {
    let (started_tx, started) = mpsc::channel();
    let (finish, finish_rx) = mpsc::channel::<()>();
    block_on(async {
        let job = spawn_blocking(move || {
            started_tx.send(()).unwrap();
            finish_rx.recv()
        });
        started.recv().unwrap();
        let err = Child::fork(&[], || 0)
            .err()
            .expect("forked during a blocking job");
        assert!(err.to_string().contains("2 threads"), "{err}");
        drop(finish);
        within(LIMIT, job).await.unwrap_err();
        let mut child = Child::fork(&[], || 0).unwrap();
        assert!(within(LIMIT, child.wait()).await.unwrap().success());
    });
}

fn pidfd_ids_tell_processes_apart() {
    let child = Child::fork(&[], forever).unwrap();
    let id = |pid| os::pidfd_id(os::pidfd_open(pid).unwrap().as_fd()).unwrap();
    assert_eq!(id(child.id()), id(child.id()));
    let own = os::own_pidfd_id().unwrap();
    assert_ne!(id(child.id()), own);
    // The same ID as pidfs's inode number, which systemd also accepts on
    // 64-bit systems.
    #[cfg(target_pointer_width = "64")]
    assert_eq!(
        os::file_identity(os::pidfd_open(std::process::id() as i32).unwrap().as_fd())
            .unwrap()
            .1,
        own
    );
}

fn main() -> std::process::ExitCode {
    common::run_single_threaded(&[
        ("wait_returns_exit_code", wait_returns_exit_code),
        (
            "wait_twice_returns_same_status",
            wait_twice_returns_same_status,
        ),
        ("killed_child_reports_signal", killed_child_reports_signal),
        ("panic_in_child_exits_101", panic_in_child_exits_101),
        (
            "dropping_a_child_kills_and_reaps_it",
            dropping_a_child_kills_and_reaps_it,
        ),
        (
            "dropping_a_child_inside_the_loop_reaps_it_in_the_background",
            dropping_a_child_inside_the_loop_reaps_it_in_the_background,
        ),
        (
            "detached_child_is_left_for_someone_else_to_reap",
            detached_child_is_left_for_someone_else_to_reap,
        ),
        ("fork_closes_fds_unless_kept", fork_closes_fds_unless_kept),
        ("kept_fds_work_in_the_child", kept_fds_work_in_the_child),
        (
            "close_in_child_closes_here_too",
            close_in_child_closes_here_too,
        ),
        (
            "forks_once_blocking_jobs_are_done",
            forks_once_blocking_jobs_are_done,
        ),
        (
            "forking_refuses_while_a_blocking_job_runs",
            forking_refuses_while_a_blocking_job_runs,
        ),
        (
            "pidfd_ids_tell_processes_apart",
            pidfd_ids_tell_processes_apart,
        ),
        // Last: the thread it starts would make the others refuse to fork.
        (
            "forking_refuses_with_threads_running",
            forking_refuses_with_threads_running,
        ),
    ])
}
