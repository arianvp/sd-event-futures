//! Reactor primitives on top of sd-event sources.
//!
//! Constructors find the loop via [`crate::Event::thread_default()`], so they
//! implicitly run on the same loop the executor is driving.
//!
//! - [`sleep`] and [`child`] are one-shot futures: build one, `.await` it
//!   once. Dropping one cancels its source.
//! - [`AsyncFd`] registers an fd with the loop once and tracks read and
//!   write readiness separately, so any number of tasks can wait on the
//!   same fd in either direction.
//! - [`connect_tcp`] and [`connect_unix`] connect stream sockets without
//!   blocking the loop.
//! - [`Signal`], [`Inotify`] and [`MemoryPressure`] keep their source
//!   alive for as long as the handle exists and queue events between
//!   awaits, so nothing is lost while the caller is busy.
//!

mod child;
mod inotify;
mod io;
mod memory_pressure;
mod net;
mod signal;
mod slots;
mod timer;
mod yield_now;

pub use child::{WaitChild, child, child_pidfd};
pub use inotify::{Inotify, InotifyEventOwned, inotify, inotify_fd};
pub use io::{AsyncFd, Readiness};
pub use memory_pressure::{MemoryPressure, memory_pressure};
pub use net::{connect_tcp, connect_unix};
pub use signal::{Signal, signal};
pub use slots::Recv;
pub use timer::{Sleep, sleep, sleep_relative, sleep_until};
pub use yield_now::{YieldNow, yield_now};

fn current_event() -> std::io::Result<crate::Event> {
    crate::Event::thread_default()
}
