//! Multi-task futures executor on top of `sd-event`, woken through
//! [`LocalWaker`]s.
//!
//! Same API and guarantees as the default executor, selected with the
//! `local-waker` cargo feature. A task's waker is an `Rc` pointing
//! straight at the task, so waking and cloning wakers involves no atomics,
//! locks or lookups.
//!
//! The price is that nothing can wake a task from another thread. Every
//! `Context` still carries a [`Waker`], because the API requires one, but
//! it panics when cloned or woken: futures must park
//! `cx.local_waker()` (see [`crate::waker::of`]).

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, VecDeque};
use std::future::{Future, poll_fn};
use std::io;
use std::panic::{self, AssertUnwindSafe};
use std::pin::{Pin, pin};
use std::rc::{Rc, Weak};
use std::sync::{Arc, Mutex};
use std::task::{
    Context, ContextBuilder, LocalWake, LocalWaker, Poll, RawWaker, RawWakerVTable, Waker,
};

use crate::os::EventFd;
use crate::reactor::AsyncFd;
use crate::{Enabled, Event, EventSource};

type TaskId = u64;

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

    let root = Rc::new(RootWaker {
        woken: Cell::new(true),
        ex: Rc::downgrade(&ex),
    });
    let local_waker = LocalWaker::from(root.clone());
    let mut cx = ContextBuilder::from_waker(&LOCAL_ONLY)
        .local_waker(&local_waker)
        .build();
    let mut future = pin!(future);
    loop {
        if root.woken.replace(false)
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

    let task = Rc::new(Task {
        id,
        future: RefCell::new(Some(wrapped)),
        queued: Cell::new(false),
        aborted: Cell::new(false),
        ex: Rc::downgrade(&ex),
    });
    ex.tasks.borrow_mut().insert(id, task.clone());
    task.wake_by_ref();

    Some(JoinHandle { task, state })
}

/// Run `f` on the [blocking pool](crate::blocking) and resolve to its
/// result, without blocking the event loop.
///
/// A pool thread can't wake a `LocalWaker`, so each call brings an
/// eventfd for the thread to signal, which the loop watches like any
/// other fd. `f` starts right away; a panic in it is re-raised in the
/// awaiting task.
pub fn spawn_blocking<T, F>(f: F) -> impl Future<Output = T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let done = Arc::new(EventFd::new().expect("eventfd"));
    let slot = Arc::new(Mutex::new(None));
    {
        let (done, slot) = (done.clone(), slot.clone());
        crate::blocking::submit(move || {
            *slot.lock().unwrap() = Some(panic::catch_unwind(AssertUnwindSafe(f)));
            done.signal();
        });
    }
    async move {
        let done = AsyncFd::new(done).expect("register eventfd");
        done.read_with(|eventfd| eventfd.reset())
            .await
            .expect("read eventfd");
        match slot
            .lock()
            .unwrap()
            .take()
            .expect("signalled without a result")
        {
            Ok(v) => v,
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
    Waiting(Option<LocalWaker>),
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
            Slot::Waiting(Some(w)) if w.will_wake(cx.local_waker()) => Poll::Pending,
            Slot::Waiting(w) => {
                *w = Some(cx.local_waker().clone());
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
    event: Event,
    queue: RefCell<VecDeque<Rc<Task>>>,
    /// Every unfinished task, so they can all be dropped on exit.
    tasks: RefCell<HashMap<TaskId, Rc<Task>>>,
    next_id: Cell<TaskId>,
}

/// A task is its own waker. The future is set to `None` as soon as the
/// task finishes or is aborted, which also drops every waker the future
/// had handed out, so wakers never keep a dead task's future alive.
struct Task {
    id: TaskId,
    future: RefCell<Option<Pin<Box<dyn Future<Output = ()>>>>>,
    queued: Cell<bool>,
    aborted: Cell<bool>,
    ex: Weak<Executor>,
}

impl LocalWake for Task {
    fn wake(self: Rc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Rc<Self>) {
        if !self.queued.replace(true)
            && let Some(ex) = self.ex.upgrade()
        {
            ex.queue.borrow_mut().push_back(self.clone());
            let _ = ex.scheduler.set_enabled(Enabled::OneShot);
        }
    }
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
        if let Some(ex) = self.ex.upgrade() {
            ex.tasks.borrow_mut().remove(&self.id);
        }
    }
}

struct RootWaker {
    woken: Cell<bool>,
    ex: Weak<Executor>,
}

impl LocalWake for RootWaker {
    fn wake(self: Rc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Rc<Self>) {
        if !self.woken.replace(true)
            && let Some(ex) = self.ex.upgrade()
        {
            // Makes the pending `sd_event_run` return to `block_on`.
            let _ = ex.scheduler.set_enabled(Enabled::OneShot);
        }
    }
}

/// The `Waker` every `Context` must carry. Futures that try to use it
/// instead of the local waker fail loudly rather than never waking.
// SAFETY: the vtable never touches the (null) data pointer, and its
// functions are fine to call from any thread: they only panic.
static LOCAL_ONLY: Waker =
    unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &LOCAL_ONLY_VTABLE)) };

static LOCAL_ONLY_VTABLE: RawWakerVTable =
    RawWakerVTable::new(local_only_clone, local_only_wake, local_only_wake, |_| {});

const LOCAL_ONLY_MSG: &str =
    "this executor only supports LocalWaker: park cx.local_waker(), not cx.waker()";

fn local_only_clone(_: *const ()) -> RawWaker {
    panic!("{LOCAL_ONLY_MSG}");
}

fn local_only_wake(_: *const ()) {
    panic!("{LOCAL_ONLY_MSG}");
}

fn current() -> Option<Rc<Executor>> {
    CURRENT.with(|c| c.borrow().clone())
}

impl Executor {
    fn new() -> io::Result<Self> {
        let event = Event::thread_default()?;
        let scheduler = event.add_defer(|| {
            if let Some(ex) = current() {
                ex.run_ready();
            }
            0
        })?;
        scheduler.set_enabled(Enabled::Off)?;
        Ok(Self {
            scheduler,
            event,
            queue: RefCell::new(VecDeque::new()),
            tasks: RefCell::new(HashMap::new()),
            next_id: Cell::new(0),
        })
    }

    /// Poll every task that was queued when this call started.
    fn run_ready(&self) {
        let batch = std::mem::take(&mut *self.queue.borrow_mut());
        for task in batch {
            // A panicking `Drop` of a task's future must not lose the rest
            // of the batch: those tasks are already marked as queued and
            // would never be queued again.
            let _ = panic::catch_unwind(AssertUnwindSafe(|| self.run_task(&task)));
        }
    }

    fn run_task(&self, task: &Rc<Task>) {
        task.queued.set(false);
        let mut slot = task.future.borrow_mut();
        let Some(future) = slot.as_mut() else {
            return;
        };
        let local_waker = LocalWaker::from(task.clone());
        let mut cx = ContextBuilder::from_waker(&LOCAL_ONLY)
            .local_waker(&local_waker)
            .build();
        let done = future.as_mut().poll(&mut cx).is_ready();
        if done || task.aborted.get() {
            let future = slot.take();
            drop(slot);
            drop(future);
            self.tasks.borrow_mut().remove(&task.id);
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
        self.0.queue.borrow_mut().clear();
        let ex = CURRENT.with(|c| c.borrow_mut().take());
        drop(ex);
    }
}
