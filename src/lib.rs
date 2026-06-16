//! Futures on top of libsystemd's [`sd-event`] event loop.
//!
//! - [`Event`] and [`EventSource`] wrap all of `sd-event.h` safely.
//! - [`executor`] runs futures on the thread's default loop:
//!   [`block_on`](executor::block_on), [`spawn`](executor::spawn) and
//!   [`spawn_blocking`](executor::spawn_blocking), which runs blocking
//!   calls on the [`blocking`] thread pool.
//! - [`reactor`] turns event sources into futures: timers, file
//!   descriptors ([`AsyncFd`](reactor::AsyncFd)), signals, child
//!   processes, inotify and memory pressure.
//! - [`combinator`] and [`sync`] combine and coordinate tasks: `select!`,
//!   [`race`](combinator::race), [`join_all`](combinator::join_all),
//!   [`Gate`](sync::Gate), [`TaskTracker`](sync::TaskTracker).
//! - [`process`] forks children and awaits them, [`forward`] moves data
//!   between sockets with `splice(2)`, and [`daemon`] speaks the service
//!   manager's protocols: readiness, reload, the fd store, socket
//!   activation and logging.
//! - [`os`] holds the safe wrappers around raw system calls that all of
//!   this builds on.
//!
//! The programs in `examples/` put all of this to work, porting systemd
//! components; `README.md` lists them.
//!
//! [`sd-event`]: https://man.archlinux.org/man/sd-event.3.en
#![cfg_attr(feature = "local-waker", feature(local_waker))]

pub mod blocking;
pub mod combinator;
pub mod daemon;
mod event;
#[cfg_attr(feature = "local-waker", path = "executor_local.rs")]
pub mod executor;
pub mod forward;
pub mod os;
pub mod process;
pub mod reactor;
pub mod sync;
pub mod sys;
pub mod waker;

pub use event::{
    Enabled, Event, EventSource, InotifyEvent, MemoryPressureType, SIGNAL_PROCMASK, State, clock,
    priority, trim_memory,
};
