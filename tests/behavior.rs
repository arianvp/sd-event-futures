//! Tier 3 — behavioral tests for the safe wrapper.
//!
//! Each test creates its own [`Event`] so tests are independent. Tests that
//! touch shared process state (signals) take care to use targeted
//! delivery and process-mask blocking via the `SIGNAL_PROCMASK` flag.
//!
//! Not covered here:
//!   - `add_child` / `add_child_pidfd`: require disabling kernel autoreap
//!     and process-wide SIGCHLD blocking, which don't compose with cargo
//!     test's threading model. Worth a separate `#[ignore]`d test or an
//!     exec-based integration harness.
//!   - `add_memory_pressure`: needs PSI / cgroup config to fire
//!     deterministically.
#![deny(unsafe_code)]

use std::cell::{Cell, RefCell};
use std::ffi::CString;
use std::io::{Read, Write};
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::net::UnixStream;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use sd_event_futures::{
    Enabled, Event, EventSource, InotifyEvent, MemoryPressureType, SIGNAL_PROCMASK, clock, priority,
};

const ITER_TIMEOUT_US: u64 = 5_000_000; // 5 s upper bound per iteration

// =============================================================================
// Defer / post / exit
// =============================================================================

#[test]
fn defer_fires_exactly_once() {
    let event = Event::new().unwrap();
    let count = Rc::new(Cell::new(0u32));

    let counter = count.clone();
    let _defer = event
        .add_defer(move || {
            counter.set(counter.get() + 1);
            0
        })
        .unwrap();

    // Defer fires immediately on the first iteration.
    assert!(event.run_once(Some(ITER_TIMEOUT_US)).unwrap());
    assert_eq!(count.get(), 1);

    // Default enable mode is ONESHOT; subsequent iterations should not fire it.
    let _ = event.run_once(Some(0));
    assert_eq!(count.get(), 1);
}

#[test]
fn post_does_not_fire_without_other_source() {
    let event = Event::new().unwrap();
    let post_count = Rc::new(Cell::new(0u32));

    let pc = post_count.clone();
    let _post = event
        .add_post(move || {
            pc.set(pc.get() + 1);
            0
        })
        .unwrap();

    // With only a post source pending, the loop has nothing to dispatch
    // and run_once should time out without firing the post.
    let dispatched = event.run_once(Some(50_000)).unwrap();
    assert!(
        !dispatched,
        "loop should have timed out with only a post source"
    );
    assert_eq!(post_count.get(), 0);
}

#[test]
fn exit_source_runs_during_shutdown() {
    let event = Event::new().unwrap();
    let order = Rc::new(std::cell::RefCell::new(Vec::<&'static str>::new()));

    let o = order.clone();
    let _exit = event
        .add_exit(move || {
            o.borrow_mut().push("exit");
            0
        })
        .unwrap();

    let o = order.clone();
    let ev = event.clone();
    let _defer = event
        .add_defer(move || {
            o.borrow_mut().push("defer");
            ev.exit(0).unwrap();
            0
        })
        .unwrap();

    event.run().unwrap();
    assert_eq!(*order.borrow(), ["defer", "exit"]);
}

// =============================================================================
// Timer
// =============================================================================

#[test]
fn timer_fires_close_to_deadline() {
    let event = Event::new().unwrap();
    let fired = Rc::new(Cell::new(false));

    let f = fired.clone();
    let ev = event.clone();
    let started = Instant::now();
    // 30 ms with 0 accuracy = "as precise as the kernel allows" (1 by spec,
    // but sd-event interprets 0 as default 250ms accuracy). Use accuracy=1
    // to force tight timing for this test.
    let _t = event
        .add_time_relative(clock::MONOTONIC, 30_000, 1, move |_usec| {
            f.set(true);
            ev.exit(0).unwrap();
            0
        })
        .unwrap();

    event.run().unwrap();
    let elapsed = started.elapsed();
    assert!(fired.get(), "timer didn't fire");
    assert!(
        elapsed >= Duration::from_millis(25),
        "timer fired too early: {elapsed:?}",
    );
    assert!(
        elapsed < Duration::from_secs(1),
        "timer fired too late: {elapsed:?}",
    );
}

#[test]
fn timer_get_set_roundtrips() {
    let event = Event::new().unwrap();
    let src = event.add_time(clock::MONOTONIC, 12345, 1, |_| 0).unwrap();
    assert_eq!(src.time().unwrap(), 12345);
    assert_eq!(src.time_clock().unwrap(), clock::MONOTONIC);

    src.set_time(99999).unwrap();
    assert_eq!(src.time().unwrap(), 99999);

    src.set_time_accuracy(42).unwrap();
    assert_eq!(src.time_accuracy().unwrap(), 42);
}

// =============================================================================
// I/O
// =============================================================================

#[test]
fn io_source_fires_on_epollin() {
    let event = Event::new().unwrap();
    let (rd, wr) = UnixStream::pair().unwrap();
    let saw = Rc::new(Cell::new(0u32));

    let s = saw.clone();
    let ev = event.clone();
    let rd_fd = rd.as_raw_fd();
    let _io = event
        .add_io(rd_fd, libc::EPOLLIN as u32, move |fd, revents| {
            assert_eq!(fd, rd_fd);
            assert!(revents & libc::EPOLLIN as u32 != 0);
            s.set(s.get() + 1);
            // Drain so we don't keep refiring.
            let mut buf = [0u8; 64];
            let _ = (&rd).read(&mut buf);
            ev.exit(0).unwrap();
            0
        })
        .unwrap();

    // Write from the other end to wake the loop.
    (&wr).write_all(b"hello").unwrap();

    event.run().unwrap();
    assert_eq!(saw.get(), 1);
}

#[test]
fn io_source_swap_fd() {
    let event = Event::new().unwrap();
    let (_a, _b) = UnixStream::pair().unwrap();
    let (c, _d) = UnixStream::pair().unwrap();

    let src = event
        .add_io(_a.as_raw_fd(), libc::EPOLLIN as u32, |_, _| 0)
        .unwrap();
    assert_eq!(src.io_fd().unwrap(), _a.as_raw_fd());

    src.set_io_fd(c.as_raw_fd()).unwrap();
    assert_eq!(src.io_fd().unwrap(), c.as_raw_fd());

    src.set_io_events(libc::EPOLLOUT as u32).unwrap();
    assert_eq!(src.io_events().unwrap(), libc::EPOLLOUT as u32);
}

// =============================================================================
// Signals
// =============================================================================

#[test]
fn signal_source_fires_on_raise() {
    let event = Event::new().unwrap();
    let saw = Rc::new(Cell::new(0u32));
    let s = saw.clone();
    let ev = event.clone();

    let src = event
        .add_signal(libc::SIGUSR1 | SIGNAL_PROCMASK, move |si| {
            assert_eq!(si.ssi_signo as i32, libc::SIGUSR1);
            s.set(s.get() + 1);
            ev.exit(0).unwrap();
            0
        })
        .unwrap();
    assert_eq!(src.signal().unwrap(), libc::SIGUSR1);

    // Deliver to the calling thread; SIGNAL_PROCMASK already blocked it
    // there so the kernel queues it for signalfd to pick up.
    sd_event_futures::os::raise(libc::SIGUSR1);

    event.run().unwrap();
    assert_eq!(saw.get(), 1);
}

// =============================================================================
// Inotify
// =============================================================================

#[test]
fn inotify_source_sees_file_creation() {
    let tmp =
        std::env::temp_dir().join(format!("sd-event-futures-inotify-{}", std::process::id(),));
    std::fs::create_dir_all(&tmp).unwrap();

    let event = Event::new().unwrap();
    let name = Rc::new(std::cell::RefCell::new(None::<String>));
    let mask = Rc::new(Cell::new(0u32));

    let n = name.clone();
    let m = mask.clone();
    let ev = event.clone();
    let path_cstr = CString::new(tmp.to_str().unwrap()).unwrap();
    let _watch = event
        .add_inotify(&path_cstr, libc::IN_CREATE, move |evt: &InotifyEvent| {
            if let Some(s) = evt.name() {
                *n.borrow_mut() = Some(s.to_string_lossy().into_owned());
            }
            m.set(evt.mask());
            ev.exit(0).unwrap();
            0
        })
        .unwrap();

    // Create a file inside the watched directory.
    let file_path = tmp.join("trigger");
    std::fs::File::create(&file_path).unwrap();

    event.run().unwrap();

    assert_eq!(name.borrow().as_deref(), Some("trigger"));
    assert!(mask.get() & libc::IN_CREATE != 0);

    let _ = std::fs::remove_dir_all(&tmp);
}

// =============================================================================
// Priority
// =============================================================================

#[test]
fn higher_priority_dispatches_first() {
    let event = Event::new().unwrap();
    let order = Rc::new(std::cell::RefCell::new(Vec::<&'static str>::new()));

    let o = order.clone();
    let low = event
        .add_defer(move || {
            o.borrow_mut().push("low");
            0
        })
        .unwrap();
    low.set_priority(priority::IDLE).unwrap();

    let o = order.clone();
    let high = event
        .add_defer(move || {
            o.borrow_mut().push("high");
            0
        })
        .unwrap();
    high.set_priority(priority::IMPORTANT).unwrap();

    // Drain both defers. They were both pending before the first iteration.
    for _ in 0..2 {
        event.run_once(Some(ITER_TIMEOUT_US)).unwrap();
    }
    assert_eq!(*order.borrow(), ["high", "low"]);
}

// =============================================================================
// Prepare callback
// =============================================================================

#[test]
fn prepare_callback_runs_before_dispatch() {
    let event = Event::new().unwrap();
    let prepared = Rc::new(Cell::new(0u32));
    let dispatched = Rc::new(Cell::new(0u32));

    let d = dispatched.clone();
    let src = event
        .add_defer(move || {
            d.set(d.get() + 1);
            0
        })
        .unwrap();

    let p = prepared.clone();
    src.set_prepare(move || {
        p.set(p.get() + 1);
        0
    })
    .unwrap();

    event.run_once(Some(ITER_TIMEOUT_US)).unwrap();
    assert!(prepared.get() >= 1, "prepare didn't fire");
    assert_eq!(dispatched.get(), 1);
}

#[test]
fn handler_can_install_prepare_on_its_own_source() {
    let event = Event::new().unwrap();
    let this: Rc<RefCell<Option<EventSource>>> = Rc::default();
    let prepared = Rc::new(Cell::new(0u32));
    let installed = Rc::new(Cell::new(false));

    let (t, p, i) = (this.clone(), prepared.clone(), installed.clone());
    let src = event
        .add_defer(move || {
            let p = p.clone();
            let src = t.borrow();
            src.as_ref()
                .unwrap()
                .set_prepare(move || {
                    p.set(p.get() + 1);
                    0
                })
                .unwrap();
            i.set(true);
            0
        })
        .unwrap();
    src.set_enabled(Enabled::On).unwrap();
    *this.borrow_mut() = Some(src.clone());

    event.run_once(Some(ITER_TIMEOUT_US)).unwrap();
    assert!(installed.get());
    event.run_once(Some(ITER_TIMEOUT_US)).unwrap();
    assert!(prepared.get() >= 1, "installed prepare didn't fire");

    this.borrow_mut().take();
}

#[test]
fn prepare_cannot_replace_itself_while_running() {
    let event = Event::new().unwrap();
    let src = event.add_defer(|| 0).unwrap();
    let this: Rc<RefCell<Option<EventSource>>> = Rc::new(RefCell::new(Some(src.clone())));
    let result: Rc<RefCell<Option<std::io::Result<()>>>> = Rc::default();

    let (t, r) = (this.clone(), result.clone());
    src.set_prepare(move || {
        let src = t.borrow();
        *r.borrow_mut() = Some(src.as_ref().unwrap().set_prepare(|| 0));
        0
    })
    .unwrap();

    event.run_once(Some(ITER_TIMEOUT_US)).unwrap();
    let err = result
        .borrow_mut()
        .take()
        .expect("prepare didn't fire")
        .unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::EBUSY));

    this.borrow_mut().take();
}

// =============================================================================
// Ratelimit
// =============================================================================

#[test]
fn ratelimit_get_and_clear() {
    let event = Event::new().unwrap();
    let src = event.add_defer(|| 0).unwrap();

    assert_eq!(src.ratelimit().unwrap(), None);

    src.set_ratelimit(1_000_000, 5).unwrap();
    assert_eq!(src.ratelimit().unwrap(), Some((1_000_000, 5)));

    // Disable.
    src.set_ratelimit(0, 0).unwrap();
    assert_eq!(src.ratelimit().unwrap(), None);
}

// =============================================================================
// Lifecycle: closure dropped exactly when source is freed
// =============================================================================

struct DropFlag(Arc<AtomicU32>);
impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn closure_dropped_when_source_dropped() {
    let event = Event::new().unwrap();
    let drops = Arc::new(AtomicU32::new(0));

    {
        let flag = DropFlag(drops.clone());
        let src: EventSource = event
            .add_defer(move || {
                // The closure owns `flag`. When the source is dropped (and
                // its destroy callback fires), the closure box is freed,
                // which drops `flag`, which bumps the counter.
                let _ = &flag;
                0
            })
            .unwrap();
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(src);
    }

    // The destroy callback runs synchronously inside sd_event_source_unref
    // when refcount drops to zero (which is our case here).
    assert_eq!(drops.load(Ordering::SeqCst), 1);

    drop(event);
}

// NOTE: `EventSource::into_floating` retains the source-to-loop reference
// established when the source was created non-floating, so the loop will
// not be torn down by dropping the caller's `Event` handle alone — the
// caller must drive the loop to completion (or rely on the loop having
// other refs that get dropped). That asymmetry matches sd-event's design
// where the canonical floating-source pattern is to construct floating
// from the start; see `sd_event_source_set_floating(3)`.
//
// We don't have a constructor for "create floating from the start"
// because the wrapper always wants a destroy-callback to free the
// closure. If you need fire-and-forget floating sources, prefer
// `add_defer` etc., set whatever options you need, and call
// `into_floating()` — but understand the loop will outlive your local
// `Event` handle.

// =============================================================================
// Panic in trampoline → source disabled, loop continues
// =============================================================================

#[test]
fn panic_in_handler_disables_source_not_process() {
    let event = Event::new().unwrap();

    // First source: panics on first dispatch.
    let panicker = event
        .add_defer(|| panic!("intentional panic in handler"))
        .unwrap();
    panicker.set_enabled(Enabled::On).unwrap();

    // Second source: counts and exits the loop.
    let count = Rc::new(Cell::new(0u32));
    let c = count.clone();
    let ev = event.clone();
    let _normal = event
        .add_defer(move || {
            c.set(c.get() + 1);
            ev.exit(0).unwrap();
            0
        })
        .unwrap();

    // Should complete without aborting the process.
    event.run().unwrap();

    // The panicker has been disabled because its trampoline returned -EIO.
    assert_eq!(panicker.enabled().unwrap(), Enabled::Off);
    // The second source ran exactly once.
    assert_eq!(count.get(), 1);
}

// =============================================================================
// Negative return disables source (default behavior)
// =============================================================================

#[test]
fn negative_handler_return_disables_source() {
    let event = Event::new().unwrap();

    let src = event.add_defer(|| -1).unwrap();
    src.set_enabled(Enabled::On).unwrap();
    assert_eq!(src.enabled().unwrap(), Enabled::On);

    // Drive one iteration so the defer dispatches.
    event.run_once(Some(ITER_TIMEOUT_US)).unwrap();

    // sd-event default policy: negative return disables the source.
    assert_eq!(src.enabled().unwrap(), Enabled::Off);
}

// =============================================================================
// Loop introspection
// =============================================================================

#[test]
fn loop_iteration_and_state_advance() {
    let event = Event::new().unwrap();
    assert_eq!(event.iteration().unwrap(), 0);

    let ev = event.clone();
    let _d = event
        .add_defer(move || {
            ev.exit(0).unwrap();
            0
        })
        .unwrap();

    event.run().unwrap();
    assert!(event.iteration().unwrap() >= 1);
    assert_eq!(event.exit_code().unwrap(), Some(0));
}

#[test]
fn fd_is_valid_epoll_fd() {
    let event = Event::new().unwrap();
    let fd: RawFd = event.fd().unwrap();
    assert!(fd >= 0);
    // It must be a real, open descriptor.
    let target = std::fs::read_link(format!("/proc/self/fd/{fd}")).expect("epoll fd is open");
    assert_eq!(target.to_str(), Some("anon_inode:[eventpoll]"));
}

// =============================================================================
// Memory pressure: construction smoke test
// =============================================================================

#[test]
fn memory_pressure_default_constructs_or_returns_documented_errno() {
    let event = Event::new().unwrap();
    match event.add_memory_pressure_default() {
        Ok(src) => {
            // Source constructed. Setting type/period after libsystemd has
            // already configured them internally (or via env vars) returns
            // EBUSY — also documented; accept both Ok and EBUSY here.
            for r in [
                src.set_memory_pressure_type(MemoryPressureType::Some),
                src.set_memory_pressure_period(100_000, 1_000_000),
            ] {
                if let Err(e) = r {
                    assert_eq!(
                        e.raw_os_error(),
                        Some(libc::EBUSY),
                        "unexpected errno reconfiguring memory pressure: {e}",
                    );
                }
            }
        }
        Err(e) => {
            // Allowed errnos per `sd_event_add_memory_pressure(3)` when
            // running in environments without PSI / cgroup setup.
            let raw = e.raw_os_error().unwrap_or(0);
            assert!(
                matches!(
                    raw,
                    libc::EOPNOTSUPP
                        | libc::EHOSTDOWN
                        | libc::EBADMSG
                        | libc::ENOTTY
                        | libc::ENOENT
                        | libc::EACCES
                ),
                "unexpected errno from add_memory_pressure_default: {raw} ({e})",
            );
        }
    }
}
