//! Multi-task futures executor on top of `sd-event`.
//!
//! - [`block_on(future)`](block_on) drives a root future to completion on
//!   the calling thread's default event loop (`sd_event_default(3)`).
//! - [`spawn(future)`](spawn) launches a future concurrently and returns
//!   a [`JoinHandle<T>`].
//! - [`spawn_blocking(f)`](spawn_blocking) runs a blocking closure on its own
//!   thread and resolves to its result.
//! - Reactor primitives find the loop the same way:
//!   [`Event::thread_default()`].
//!
//! The executor owns every spawned task. When `block_on` returns, tasks
//! that have not finished are dropped, which cancels their event sources.
//!
//! Wakers are ordinary thread-safe [`Waker`]s. A wake on the loop's own
//! thread enables a defer source; a wake from any other thread writes to
//! an eventfd the loop watches. A task is queued at most once no matter
//! how often it is woken, and each scheduler dispatch only polls the
//! tasks that were queued when it started, so a task that keeps waking
//! itself cannot starve the rest of the loop.

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, VecDeque};
use std::future::{Future, poll_fn};
use std::hash::{BuildHasherDefault, Hasher};
use std::io;
use std::os::fd::{AsFd, AsRawFd};
use std::panic::{self, AssertUnwindSafe};
use std::pin::{Pin, pin};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

use crate::os::EventFd;
use crate::{Enabled, Event, EventSource};

type TaskId = u64;

/// Id of the future passed to [`block_on`]. It is polled by `block_on`
/// itself, never from the task map.
const ROOT: TaskId = 0;

thread_local! {
    static CURRENT: RefCell<Option<Rc<Executor>>> = const { RefCell::new(None) };
}

// =============================================================================
// Public API
// =============================================================================

/// Run `future` to completion on the calling thread's default
/// `sd_event` loop. Returns the future's output.
///
/// The root future is only re-polled after its waker fires. Spawned
/// tasks that are still pending when it completes are dropped.
///
/// Panics if called recursively (nested `block_on`) on the same thread.
pub fn block_on<F: Future>(future: F) -> F::Output {
    let ex = Rc::new(Executor::new().expect("create executor"));
    CURRENT.with(|c| {
        let mut c = c.borrow_mut();
        assert!(c.is_none(), "block_on cannot be nested on the same thread");
        *c = Some(ex.clone());
    });
    let _guard = ExitGuard(ex.clone());

    let root = Arc::new(TaskWaker {
        id: ROOT,
        queued: AtomicBool::new(true),
        shared: ex.shared.clone(),
    });
    let waker = Waker::from(root.clone());
    let mut cx = Context::from_waker(&waker);
    let mut future = pin!(future);
    loop {
        if root.queued.swap(false, Ordering::AcqRel)
            && let Poll::Ready(v) = future.as_mut().poll(&mut cx)
        {
            return v;
        }
        ex.event.run_once(None).expect("sd_event_run");
    }
}

/// Spawn `future` on the current executor. Returns a [`JoinHandle`]
/// that resolves to the future's output. Panics outside [`block_on`].
///
/// A panic inside `future` is caught and re-raised in whoever awaits the
/// [`JoinHandle`]; it does not affect other tasks.
pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + 'static,
    F::Output: 'static,
{
    try_spawn(future).expect("spawn called outside a `block_on` scope")
}

/// Like [`spawn`], but outside [`block_on`] returns `None` and drops
/// `future` instead of panicking.
pub fn try_spawn<F>(future: F) -> Option<JoinHandle<F::Output>>
where
    F: Future + 'static,
    F::Output: 'static,
{
    let ex = current()?;
    let id = ex.next_id.get();
    ex.next_id.set(id + 1);

    let state = Rc::new(JoinState {
        slot: RefCell::new(Slot::Waiting(None)),
    });
    let state_for_task = state.clone();
    let wrapped = Box::pin(async move {
        let mut future = pin!(future);
        let result = poll_fn(|cx| {
            match panic::catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(cx))) {
                Ok(Poll::Pending) => Poll::Pending,
                Ok(Poll::Ready(v)) => Poll::Ready(Ok(v)),
                Err(payload) => Poll::Ready(Err(payload)),
            }
        })
        .await;
        state_for_task.complete(result);
    });

    let flag = Arc::new(TaskWaker {
        id,
        queued: AtomicBool::new(false),
        shared: ex.shared.clone(),
    });
    let task = Rc::new(Task {
        id,
        future: RefCell::new(Some(wrapped)),
        aborted: Cell::new(false),
        waker: Waker::from(flag.clone()),
        flag: flag.clone(),
    });
    ex.tasks.borrow_mut().insert(id, task.clone());
    flag.wake_by_ref();

    Some(JoinHandle { task, state })
}

/// Run `f` on the [blocking pool](crate::blocking) and resolve to its
/// result, without blocking the event loop.
///
/// Meant for calls that have no non-blocking form, such as
/// `getaddrinfo(3)`, `fsync(2)` or reads from regular files. The pool
/// thread wakes the awaiting task through its ordinary [`Waker`], which
/// reaches the loop through the executor's eventfd.
///
/// `f` starts as soon as a pool thread is free, whether or not the
/// returned future is polled. Dropping the future does not stop `f`; its
/// result is discarded. A panic in `f` is re-raised in the awaiting task.
/// Unlike [`spawn`], this works outside [`block_on`] too.
pub fn spawn_blocking<T, F>(f: F) -> Blocking<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let shared = Arc::new(Mutex::new(BlockingState {
        result: None,
        waker: None,
    }));
    let s = shared.clone();
    crate::blocking::submit(move || {
        let result = panic::catch_unwind(AssertUnwindSafe(f));
        let waker = {
            let mut s = s.lock().unwrap();
            s.result = Some(result);
            s.waker.take()
        };
        if let Some(w) = waker {
            w.wake();
        }
    });
    Blocking {
        shared,
        done: false,
    }
}

/// Future returned by [`spawn_blocking`].
pub struct Blocking<T> {
    shared: Arc<Mutex<BlockingState<T>>>,
    done: bool,
}

struct BlockingState<T> {
    result: Option<std::thread::Result<T>>,
    waker: Option<Waker>,
}

impl<T> Future for Blocking<T> {
    type Output = T;

    /// # Panics
    ///
    /// Re-raises a panic from the closure, and panics if polled again
    /// after it returned `Ready`.
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        assert!(!self.done, "Blocking polled after completion");
        let mut s = self.shared.lock().unwrap();
        let Some(result) = s.result.take() else {
            match &mut s.waker {
                Some(w) if w.will_wake(cx.waker()) => {}
                w => *w = Some(cx.waker().clone()),
            }
            return Poll::Pending;
        };
        drop(s);
        self.done = true;
        match result {
            Ok(v) => Poll::Ready(v),
            Err(payload) => panic::resume_unwind(payload),
        }
    }
}

// =============================================================================
// JoinHandle
// =============================================================================

/// Owned handle to a spawned task.
///
/// Awaiting it yields the task's output, or re-raises the task's panic.
/// Dropping it detaches the task: it keeps running until it finishes or
/// the enclosing [`block_on`] returns. Use [`JoinHandle::abort`] to stop
/// it early.
pub struct JoinHandle<T> {
    task: Rc<Task>,
    state: Rc<JoinState<T>>,
}

struct JoinState<T> {
    slot: RefCell<Slot<T>>,
}

enum Slot<T> {
    Waiting(Option<Waker>),
    Done(std::thread::Result<T>),
    Taken,
}

impl<T> JoinState<T> {
    fn complete(&self, result: std::thread::Result<T>) {
        let prev = self.slot.replace(Slot::Done(result));
        if let Slot::Waiting(Some(w)) = prev {
            w.wake();
        }
    }
}

impl<T> JoinHandle<T> {
    /// Whether the task has run to completion (or panicked).
    pub fn is_finished(&self) -> bool {
        !matches!(*self.state.slot.borrow(), Slot::Waiting(_))
    }

    /// Cancel the task: its future is dropped without being polled again.
    /// A task may abort itself; it is then dropped as soon as its current
    /// poll returns.
    pub fn abort(self) {
        self.task.cancel();
    }
}

impl<T> Future for JoinHandle<T> {
    type Output = T;

    /// # Panics
    ///
    /// Re-raises the task's panic, and panics if polled again after it
    /// returned `Ready`.
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        let mut slot = self.state.slot.borrow_mut();
        match &mut *slot {
            Slot::Waiting(Some(w)) if w.will_wake(cx.waker()) => Poll::Pending,
            Slot::Waiting(w) => {
                *w = Some(cx.waker().clone());
                Poll::Pending
            }
            Slot::Done(_) => {
                let Slot::Done(result) = std::mem::replace(&mut *slot, Slot::Taken) else {
                    unreachable!()
                };
                drop(slot);
                match result {
                    Ok(v) => Poll::Ready(v),
                    Err(payload) => panic::resume_unwind(payload),
                }
            }
            Slot::Taken => panic!("JoinHandle polled after completion"),
        }
    }
}

// =============================================================================
// Executor internals
// =============================================================================

struct Executor {
    scheduler: EventSource,
    _wakeup: EventSource,
    event: Event,
    shared: Arc<Shared>,
    /// Every unfinished task, so wakes (which carry only an id) can find
    /// them and so they can all be dropped on exit.
    tasks: RefCell<HashMap<TaskId, Rc<Task>, BuildHasherDefault<IdHasher>>>,
    /// Tasks woken on the loop thread; `Shared::remote_queue` holds the rest.
    local_queue: RefCell<VecDeque<TaskId>>,
    next_id: Cell<TaskId>,
}

/// The part of the executor that wakers reach from other threads.
struct Shared {
    remote_queue: Mutex<VecDeque<TaskId>>,
    remote_pending: AtomicBool,
    eventfd: EventFd,
}

/// The future is set to `None` as soon as the task finishes or is aborted.
struct Task {
    id: TaskId,
    future: RefCell<Option<Pin<Box<dyn Future<Output = ()>>>>>,
    aborted: Cell<bool>,
    flag: Arc<TaskWaker>,
    waker: Waker,
}

impl Task {
    fn cancel(&self) {
        // Borrowed means the task is being polled right now: it aborted
        // itself, and `run_task` drops it once the poll returns.
        let Ok(mut slot) = self.future.try_borrow_mut() else {
            self.aborted.set(true);
            return;
        };
        let future = slot.take();
        drop(slot);
        drop(future);
        if let Some(ex) = current() {
            ex.tasks.borrow_mut().remove(&self.id);
        }
    }
}

/// Task ids are sequential, so they make a good hash on their own.
#[derive(Default)]
struct IdHasher(u64);

impl Hasher for IdHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, _: &[u8]) {
        unreachable!("only u64 task ids are hashed")
    }

    fn write_u64(&mut self, id: u64) {
        self.0 = id;
    }
}

struct TaskWaker {
    id: TaskId,
    queued: AtomicBool,
    shared: Arc<Shared>,
}

impl Wake for TaskWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if !self.queued.swap(true, Ordering::AcqRel) {
            self.shared.schedule(self.id);
        }
    }
}

impl Shared {
    fn schedule(self: &Arc<Self>, id: TaskId) {
        // `CURRENT` only holds this executor on the thread running it.
        let on_loop_thread = CURRENT
            .try_with(|c| match c.borrow().as_ref() {
                Some(ex) if Arc::ptr_eq(&ex.shared, self) => {
                    ex.local_queue.borrow_mut().push_back(id);
                    let _ = ex.scheduler.set_enabled(Enabled::OneShot);
                    true
                }
                _ => false,
            })
            .unwrap_or(false);
        if !on_loop_thread {
            self.remote_queue.lock().unwrap().push_back(id);
            self.remote_pending.store(true, Ordering::Release);
            self.eventfd.signal();
        }
    }
}

fn current() -> Option<Rc<Executor>> {
    CURRENT.with(|c| c.borrow().clone())
}

fn run_current() {
    if let Some(ex) = current() {
        ex.run_ready();
    }
}

impl Executor {
    fn new() -> io::Result<Self> {
        let event = Event::thread_default()?;
        let shared = Arc::new(Shared {
            remote_queue: Mutex::new(VecDeque::new()),
            remote_pending: AtomicBool::new(false),
            eventfd: EventFd::new()?,
        });

        let scheduler = event.add_defer(|| {
            run_current();
            0
        })?;
        scheduler.set_enabled(Enabled::Off)?;
        let woken = shared.clone();
        let wakeup = event.add_io(
            shared.eventfd.as_fd().as_raw_fd(),
            libc::EPOLLIN as u32,
            move |_, _| {
                let _ = woken.eventfd.reset();
                run_current();
                0
            },
        )?;

        Ok(Self {
            scheduler,
            _wakeup: wakeup,
            event,
            shared,
            tasks: RefCell::new(HashMap::default()),
            local_queue: RefCell::new(VecDeque::new()),
            next_id: Cell::new(ROOT + 1),
        })
    }

    /// Poll every task that was queued when this call started.
    fn run_ready(&self) {
        let mut batch = std::mem::take(&mut *self.local_queue.borrow_mut());
        if self.shared.remote_pending.swap(false, Ordering::Acquire) {
            batch.extend(std::mem::take(
                &mut *self.shared.remote_queue.lock().unwrap(),
            ));
        }
        for id in batch {
            // A panicking `Drop` of a task's future must not lose the rest
            // of the batch: those tasks are already marked as queued and
            // would never be queued again.
            let _ = panic::catch_unwind(AssertUnwindSafe(|| self.run_task(id)));
        }
    }

    fn run_task(&self, id: TaskId) {
        // Wakes for finished or aborted tasks find nothing.
        let Some(task) = self.tasks.borrow().get(&id).cloned() else {
            return;
        };
        task.flag.queued.store(false, Ordering::Release);
        let mut slot = task.future.borrow_mut();
        let Some(future) = slot.as_mut() else {
            return;
        };
        let done = future
            .as_mut()
            .poll(&mut Context::from_waker(&task.waker))
            .is_ready();
        if done || task.aborted.get() {
            let future = slot.take();
            drop(slot);
            drop(future);
            self.tasks.borrow_mut().remove(&id);
        }
    }
}

struct ExitGuard(Rc<Executor>);

impl Drop for ExitGuard {
    fn drop(&mut self) {
        // Dropping a task can spawn new ones; keep going until none are left.
        loop {
            let tasks = std::mem::take(&mut *self.0.tasks.borrow_mut());
            if tasks.is_empty() {
                break;
            }
            for task in tasks.into_values() {
                let future = task.future.borrow_mut().take();
                drop(future);
            }
        }
        let ex = CURRENT.with(|c| c.borrow_mut().take());
        drop(ex);
    }
}
