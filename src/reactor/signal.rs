//! Signals, received through one shared signalfd source per signal.

use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::os::raw::c_int;
use std::rc::{Rc, Weak};

use libc::signalfd_siginfo;

use super::current_event;
use super::slots::{Queue, Recv};
use crate::{EventSource, SIGNAL_PROCMASK};

thread_local! {
    static SIGNALS: RefCell<HashMap<c_int, Weak<SignalSource>>> = RefCell::new(HashMap::new());
}

/// One sd-event source per signal per thread, fanned out to every
/// [`Signal`] handle for that signal.
struct SignalSource {
    signo: c_int,
    subscribers: Rc<RefCell<Vec<Weak<Queue<signalfd_siginfo>>>>>,
    _src: EventSource,
}

impl Drop for SignalSource {
    fn drop(&mut self) {
        let _ = SIGNALS.try_with(|m| {
            let mut m = m.borrow_mut();
            if m.get(&self.signo).is_some_and(|w| w.strong_count() == 0) {
                m.remove(&self.signo);
            }
        });
    }
}

/// Subscription to deliveries of one signal.
///
/// While any `Signal` for a signal number exists on this thread, that
/// signal is blocked and every delivery is queued for every handle.
/// Keep the handle alive across awaits; once the last one is dropped the
/// signal is unblocked again and gets its default disposition.
pub struct Signal {
    queue: Rc<Queue<signalfd_siginfo>>,
    _source: Rc<SignalSource>,
}

/// Subscribe to `sig` (e.g. `libc::SIGUSR1`).
///
/// The signal is blocked in the calling thread via [`SIGNAL_PROCMASK`];
/// it must also be blocked in every other thread for delivery to be
/// reliable. Several handles for the same signal share one source.
pub fn signal(sig: c_int) -> io::Result<Signal> {
    let existing = SIGNALS.with(|m| m.borrow().get(&sig).and_then(Weak::upgrade));
    let source = match existing {
        Some(source) => source,
        None => {
            let subscribers: Rc<RefCell<Vec<Weak<Queue<signalfd_siginfo>>>>> = Rc::default();
            let subs = subscribers.clone();
            let src = current_event()?.add_signal(sig | SIGNAL_PROCMASK, move |si| {
                subs.borrow_mut().retain(|q| match q.upgrade() {
                    Some(q) => {
                        q.push(*si);
                        true
                    }
                    None => false,
                });
                0
            })?;
            let source = Rc::new(SignalSource {
                signo: sig,
                subscribers,
                _src: src,
            });
            SIGNALS.with(|m| m.borrow_mut().insert(sig, Rc::downgrade(&source)));
            source
        }
    };
    let queue = Rc::new(Queue::default());
    source.subscribers.borrow_mut().push(Rc::downgrade(&queue));
    Ok(Signal {
        queue,
        _source: source,
    })
}

impl Signal {
    /// Wait for the next delivery (or take one that already arrived).
    pub fn recv(&mut self) -> Recv<'_, signalfd_siginfo> {
        self.queue.recv()
    }
}
