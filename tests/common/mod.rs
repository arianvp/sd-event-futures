//! Helpers shared by the integration tests.
#![allow(dead_code, unused_imports)]

mod activation;
mod futures;
mod notify;
mod runner;

pub use activation::socket_activated;
pub use futures::{CountPolls, DropFlag, ThreadSignal, Watchdog, within};
pub use notify::{Notification, NotifySocket};
pub(crate) use runner::tests;
pub use runner::{RUN_EXAMPLE, run_example_or, run_single_threaded, this_program};
