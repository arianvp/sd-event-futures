//! A process-wide pool of threads for blocking calls.
//!
//! Threads are started on demand, up to [`MAX_THREADS`], and reused; jobs
//! beyond that wait in a queue. A thread that has been idle for
//! [`IDLE_TIMEOUT`] exits. This mirrors sd-resolve's worker pool, which
//! also grows on demand up to 16 threads. [`retire_idle`] stops idle
//! threads at once, for a process about to fork.
//!
//! Every pool thread runs with all signals blocked, so signals keep
//! arriving through the event loop's signalfd instead of being taken by a
//! worker.
//!
//! Most code wants [`crate::executor::spawn_blocking`], which runs a
//! closure here and lets a task await its result.

use std::collections::VecDeque;
use std::mem;
use std::panic::{self, AssertUnwindSafe};
use std::sync::{Condvar, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use libc::pid_t;

use crate::os;

/// Upper bound on the number of pool threads.
pub const MAX_THREADS: usize = 16;

/// How long a pool thread waits for a new job before exiting.
pub const IDLE_TIMEOUT: Duration = Duration::from_secs(10);

type Job = Box<dyn FnOnce() + Send>;

struct State {
    queue: VecDeque<Job>,
    /// Each returns its thread id.
    workers: Vec<JoinHandle<pid_t>>,
    idle: usize,
    /// Workers started before the last [`retire_idle`] exit once the queue
    /// is empty.
    generation: u64,
}

static STATE: Mutex<State> = Mutex::new(State {
    queue: VecDeque::new(),
    workers: Vec::new(),
    idle: 0,
    generation: 0,
});
static WORK: Condvar = Condvar::new();

fn state() -> MutexGuard<'static, State> {
    // Jobs never run under the lock, so it can't be poisoned by them.
    STATE.lock().unwrap()
}

/// Run `job` on a pool thread. Returns immediately.
///
/// Panics if no pool thread exists and none can be started; a panic in
/// `job` itself is caught and only ends that job.
pub fn submit(job: impl FnOnce() + Send + 'static) {
    let mut s = state();
    s.queue.push_back(Box::new(job));
    WORK.notify_one();
    if s.queue.len() <= s.idle || s.workers.len() >= MAX_THREADS {
        return;
    }
    // Started with all signals blocked, so the thread never takes one
    // meant for an event loop's signalfd.
    let generation = s.generation;
    let spawned = os::with_all_signals_blocked(|| {
        thread::Builder::new()
            .name("blocking".into())
            .spawn(move || worker(generation))
    });
    match spawned {
        Ok(worker) => s.workers.push(worker),
        // With other threads alive the job is still picked up later.
        Err(_) if !s.workers.is_empty() => {}
        Err(e) => {
            drop(s);
            panic!("no blocking pool thread could be started: {e}");
        }
    }
}

/// Stop all pool threads and wait until they are gone, unless one of them
/// is running a job. Later jobs start new threads.
///
/// A process that only used the pool in passing is single-threaded again
/// afterwards, and may [`fork`](crate::process::Child::fork).
pub fn retire_idle() {
    let workers = {
        let mut s = state();
        if s.idle < s.workers.len() || !s.queue.is_empty() {
            return;
        }
        s.generation += 1;
        WORK.notify_all();
        mem::take(&mut s.workers)
    };
    for worker in workers {
        if let Ok(tid) = worker.join() {
            // `join()` returns once the thread has left user space; the
            // kernel lists it until it has finished exiting, moments later.
            while os::thread_exists(tid) {
                thread::yield_now();
            }
        }
    }
}

fn worker(generation: u64) -> pid_t {
    let mut s = state();
    loop {
        if let Some(job) = s.queue.pop_front() {
            drop(s);
            let _ = panic::catch_unwind(AssertUnwindSafe(job));
            s = state();
            continue;
        }
        if s.generation != generation {
            break;
        }
        s.idle += 1;
        let (guard, wait) = WORK.wait_timeout(s, IDLE_TIMEOUT).unwrap();
        s = guard;
        s.idle -= 1;
        if wait.timed_out() && s.queue.is_empty() {
            let me = thread::current().id();
            s.workers.retain(|worker| worker.thread().id() != me);
            break;
        }
    }
    os::gettid()
}
