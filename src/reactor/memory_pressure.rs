//! Memory pressure notifications.

use std::io;
use std::rc::Rc;

use super::current_event;
use super::slots::{Queue, Recv};
use crate::{Enabled, EventSource};

/// A PSI memory-pressure watch. Notifications that arrive while nobody is
/// waiting collapse into one.
pub struct MemoryPressure {
    queue: Rc<Queue<()>>,
    _src: EventSource,
}

/// Watch for memory-pressure notifications.
///
/// Fallible because PSI / cgroup configuration commonly isn't
/// available (see `sd_event_add_memory_pressure(3)` for source
/// resolution and the documented errnos).
pub fn memory_pressure() -> io::Result<MemoryPressure> {
    let queue: Rc<Queue<()>> = Rc::default();
    let q = queue.clone();
    let src = current_event()?.add_memory_pressure(move || {
        if q.is_empty() {
            q.push(());
        }
        0
    })?;
    src.set_enabled(Enabled::On)?;
    Ok(MemoryPressure { queue, _src: src })
}

impl MemoryPressure {
    /// Wait for the next notification (or take one that already arrived).
    pub fn wait(&mut self) -> Recv<'_, ()> {
        self.queue.recv()
    }
}
