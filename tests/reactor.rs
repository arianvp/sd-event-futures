//! Reactor-primitive tests, driven through `block_on`.
//!
//! Waiting for children is covered in `tests/process.rs`, memory
//! pressure in `tests/example_greeter.rs` (through `MEMORY_PRESSURE_WATCH`).
#![deny(unsafe_code)]

mod common;

use std::cell::{Cell, RefCell};
use std::ffi::CString;
use std::io::{ErrorKind, Read, Write};
use std::os::fd::{AsFd, AsRawFd, BorrowedFd};
use std::os::unix::net::UnixStream;
use std::rc::Rc;
use std::time::{Duration, Instant};

use sd_event_futures::combinator::{Either, race};
use sd_event_futures::executor::{block_on, spawn};
use sd_event_futures::reactor::{AsyncFd, child, inotify, signal, sleep, yield_now};

use common::within;

const LIMIT: Duration = Duration::from_secs(2);

fn temp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "sd-event-futures-reactor-{name}-{}",
        std::process::id(),
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

// =============================================================================
// Sleep
// =============================================================================

#[test]
fn sleep_resolves_after_duration() {
    let start = Instant::now();
    block_on(async {
        sleep(Duration::from_millis(50)).await;
    });
    let elapsed = start.elapsed();
    assert!(
        elapsed >= Duration::from_millis(40),
        "woke too early: {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_secs(2),
        "woke too late: {elapsed:?}"
    );
}

#[test]
fn two_sleeps_back_to_back() {
    let start = Instant::now();
    block_on(async {
        sleep(Duration::from_millis(20)).await;
        sleep(Duration::from_millis(20)).await;
    });
    assert!(start.elapsed() >= Duration::from_millis(35));
}

// =============================================================================
// AsyncFd
// =============================================================================

#[test]
fn async_fd_readable_when_peer_wrote_before_await() {
    let (a, mut b) = UnixStream::pair().unwrap();
    b.write_all(b"x").unwrap();
    block_on(async move {
        let a = AsyncFd::new(a).unwrap();
        within(LIMIT, a.readable()).await;
    });
}

#[test]
fn async_fd_writable_on_empty_socket() {
    let (a, _b) = UnixStream::pair().unwrap();
    block_on(async move {
        let a = AsyncFd::new(a).unwrap();
        within(LIMIT, a.writable()).await;
    });
}

#[test]
fn async_fd_read_with_waits_for_data() {
    let (a, b) = UnixStream::pair().unwrap();
    let got = block_on(async move {
        let a = AsyncFd::new(a).unwrap();
        let writer = spawn(async move {
            sleep(Duration::from_millis(20)).await;
            (&b).write_all(b"hello").unwrap();
            b
        });
        let mut buf = [0u8; 16];
        let n = within(LIMIT, a.read_with(|mut s| s.read(&mut buf)))
            .await
            .unwrap();
        writer.await;
        buf[..n].to_vec()
    });
    assert_eq!(got, b"hello");
}

#[test]
fn async_fd_waits_for_read_and_write_on_the_same_fd() {
    let (a, mut b) = UnixStream::pair().unwrap();
    block_on(async move {
        let a = AsyncFd::new(a).unwrap();
        match within(LIMIT, race(a.readable(), a.writable())).await {
            Either::Left(()) => panic!("nothing was written yet"),
            Either::Right(()) => {}
        }

        b.write_all(b"x").unwrap();
        within(LIMIT, a.readable()).await;
    });
}

#[test]
fn async_fd_reader_and_writer_tasks_share_one_fd() {
    let (a, b) = UnixStream::pair().unwrap();
    block_on(async move {
        let a = Rc::new(AsyncFd::new(a).unwrap());
        let b = Rc::new(AsyncFd::new(b).unwrap());

        // Echo task on `b`.
        let echo = spawn({
            let b = b.clone();
            async move {
                let mut buf = [0u8; 4];
                let n = b.read_with(|mut s| s.read(&mut buf)).await.unwrap();
                b.write_with(|mut s| s.write(&buf[..n])).await.unwrap();
            }
        });
        // `reader` parks on `a` before `writer` writes to the same fd.
        let reader = spawn({
            let a = a.clone();
            async move {
                let mut buf = [0u8; 4];
                let n = a.read_with(|mut s| s.read(&mut buf)).await.unwrap();
                buf[..n].to_vec()
            }
        });
        let writer = spawn({
            let a = a.clone();
            async move {
                yield_now().await;
                a.write_with(|mut s| s.write(b"ping")).await.unwrap()
            }
        });

        assert_eq!(within(LIMIT, writer).await, 4);
        within(LIMIT, echo).await;
        assert_eq!(within(LIMIT, reader).await, b"ping");
    });
}

#[test]
fn async_fd_wakes_every_waiting_reader() {
    let (a, mut b) = UnixStream::pair().unwrap();
    block_on(async move {
        let a = Rc::new(AsyncFd::new(a).unwrap());
        let woken = Rc::new(Cell::new(0));
        let tasks: Vec<_> = (0..3)
            .map(|_| {
                let (a, woken) = (a.clone(), woken.clone());
                spawn(async move {
                    a.readable().await;
                    woken.set(woken.get() + 1);
                })
            })
            .collect();
        yield_now().await;
        b.write_all(b"x").unwrap();
        for t in tasks {
            within(LIMIT, t).await;
        }
        assert_eq!(woken.get(), 3);
    });
}

#[test]
fn async_fd_rejects_regular_file() {
    let dir = temp_dir("regular-file");
    let file = std::fs::File::create(dir.join("f")).unwrap();
    block_on(async move {
        let err = AsyncFd::new(file)
            .err()
            .expect("regular files can't be polled");
        assert_eq!(err.kind(), ErrorKind::PermissionDenied);
    });
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn async_fd_rejects_second_registration_of_fd() {
    let (a, _b) = UnixStream::pair().unwrap();
    block_on(async move {
        let first = AsyncFd::new(a).unwrap();
        let fd: BorrowedFd<'_> = first.get_ref().as_fd();
        let err = AsyncFd::new(fd).err().expect("fd is already registered");
        assert_eq!(err.kind(), ErrorKind::AlreadyExists);
    });
}

#[test]
fn async_fd_into_inner_deregisters() {
    let (a, _b) = UnixStream::pair().unwrap();
    block_on(async move {
        let a = AsyncFd::new(a).unwrap().into_inner();
        let a = AsyncFd::new(a).expect("re-register after into_inner");
        assert!(a.get_ref().as_raw_fd() >= 0);
    });
}

// =============================================================================
// Signal
// =============================================================================

#[test]
fn signal_resolves_when_raised() {
    let si = block_on(async {
        let mut sig = signal(libc::SIGUSR1).unwrap();
        sd_event_futures::os::raise(libc::SIGUSR1);
        within(LIMIT, sig.recv()).await
    });
    assert_eq!(si.ssi_signo as i32, libc::SIGUSR1);
}

#[test]
fn signal_is_delivered_to_every_handle() {
    block_on(async {
        let mut first = signal(libc::SIGUSR2).unwrap();
        let mut second = signal(libc::SIGUSR2).expect("second handle for the same signal");
        sd_event_futures::os::raise(libc::SIGUSR2);
        let a = within(LIMIT, first.recv()).await;
        let b = within(LIMIT, second.recv()).await;
        assert_eq!(a.ssi_signo as i32, libc::SIGUSR2);
        assert_eq!(b.ssi_signo as i32, libc::SIGUSR2);
    });
}

#[test]
fn signal_raised_between_receives_is_queued() {
    // SIGALRM terminates the process if it is ever left unblocked.
    block_on(async {
        let mut sig = signal(libc::SIGALRM).unwrap();
        for _ in 0..3 {
            sd_event_futures::os::raise(libc::SIGALRM);
            sleep(Duration::from_millis(5)).await;
            let si = within(LIMIT, sig.recv()).await;
            assert_eq!(si.ssi_signo as i32, libc::SIGALRM);
        }
    });
}

// =============================================================================
// Child
// =============================================================================

#[test]
fn child_reports_invalid_pid_as_error() {
    block_on(async {
        let err = child(-1, libc::WEXITED).err().expect("pid -1 is invalid");
        assert_eq!(err.raw_os_error(), Some(libc::EINVAL));
    });
}

// =============================================================================
// Inotify
// =============================================================================

#[test]
fn inotify_resolves_on_file_creation() {
    let tmp = temp_dir("inotify");
    let path = CString::new(tmp.to_str().unwrap()).unwrap();
    let tmp_clone = tmp.clone();
    let ev = block_on(async move {
        let mut watcher = inotify(&path, libc::IN_CREATE).unwrap();
        std::fs::File::create(tmp_clone.join("trigger")).unwrap();
        within(LIMIT, watcher.recv()).await
    });
    assert!(ev.mask & libc::IN_CREATE != 0);
    assert_eq!(
        ev.name.as_deref().map(|c| c.to_string_lossy().into_owned()),
        Some("trigger".to_string()),
    );

    let _ = std::fs::remove_dir_all(&tmp);
}

#[test]
fn inotify_queues_events_between_awaits() {
    let tmp = temp_dir("inotify-queue");
    let path = CString::new(tmp.to_str().unwrap()).unwrap();
    let tmp_clone = tmp.clone();
    let names = block_on(async move {
        let mut watcher = inotify(&path, libc::IN_CREATE).unwrap();
        let mut names = Vec::new();
        std::fs::File::create(tmp_clone.join("one")).unwrap();
        names.push(within(LIMIT, watcher.recv()).await.name);
        std::fs::File::create(tmp_clone.join("two")).unwrap();
        std::fs::File::create(tmp_clone.join("three")).unwrap();
        sleep(Duration::from_millis(20)).await;
        names.push(within(LIMIT, watcher.recv()).await.name);
        names.push(within(LIMIT, watcher.recv()).await.name);
        names
    });
    let names: Vec<_> = names
        .into_iter()
        .map(|n| n.unwrap().into_string().unwrap())
        .collect();
    assert_eq!(names, ["one", "two", "three"]);

    let _ = std::fs::remove_dir_all(&tmp);
}

// =============================================================================
// Yield
// =============================================================================

#[test]
fn yield_now_passes_through_to_next_iteration() {
    let stages = Rc::new(Cell::new(0u32));
    let s = stages.clone();
    block_on(async move {
        s.set(s.get() + 1);
        yield_now().await;
        s.set(s.get() + 1);
        yield_now().await;
        s.set(s.get() + 1);
    });
    assert_eq!(stages.get(), 3);
}

#[test]
fn yield_now_interleaves_tasks() {
    let log = Rc::new(RefCell::new(String::new()));
    block_on({
        let log = log.clone();
        async move {
            let tasks: Vec<_> = ['a', 'b']
                .into_iter()
                .map(|c| {
                    let log = log.clone();
                    spawn(async move {
                        for _ in 0..3 {
                            log.borrow_mut().push(c);
                            yield_now().await;
                        }
                    })
                })
                .collect();
            for t in tasks {
                within(LIMIT, t).await;
            }
        }
    });
    assert_eq!(*log.borrow(), "ababab");
}
