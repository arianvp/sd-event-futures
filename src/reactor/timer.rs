//! Timers.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use std::time::Duration;

use libc::clockid_t;

use super::current_event;
use super::slots::WakeSlot;
use crate::{EventSource, clock};

/// Future that resolves when a timer expires.
pub struct Sleep {
    slot: Rc<WakeSlot<()>>,
    _src: EventSource,
}

/// Suspend for `dur` on `CLOCK_MONOTONIC`.
///
/// Panics if the timer cannot be created (see [`sleep_relative`]).
pub fn sleep(dur: Duration) -> Sleep {
    let usec = dur.as_micros().min(u64::MAX as u128) as u64;
    sleep_relative(clock::MONOTONIC, usec)
}

/// Suspend until `deadline_us` microseconds since `clock`'s epoch.
///
/// Panics if the timer cannot be created, e.g. for an invalid clock.
pub fn sleep_until(clock: clockid_t, deadline_us: u64) -> Sleep {
    timer(clock, deadline_us, false).expect("sd_event_add_time")
}

/// Suspend for `usec` microseconds relative to the loop's current
/// iteration time on `clock`.
///
/// Panics if the timer cannot be created, e.g. for an invalid clock.
pub fn sleep_relative(clock: clockid_t, usec: u64) -> Sleep {
    timer(clock, usec, true).expect("sd_event_add_time_relative")
}

fn timer(clock: clockid_t, usec: u64, relative: bool) -> io::Result<Sleep> {
    let slot = Rc::new(WakeSlot::default());
    let s = slot.clone();
    let callback = move |_| {
        s.complete(());
        0
    };
    let event = current_event()?;
    let src = if relative {
        event.add_time_relative(clock, usec, 1, callback)?
    } else {
        event.add_time(clock, usec, 1, callback)?
    };
    Ok(Sleep { slot, _src: src })
}

impl Future for Sleep {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        self.slot.poll(cx)
    }
}
