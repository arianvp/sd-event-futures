//! The waker type that futures in this crate park while pending.
//!
//! A thread-safe [`std::task::Waker`] by default; with the `local-waker`
//! feature, a [`std::task::LocalWaker`], matching the executor in use.

use std::task::Context;

#[cfg(feature = "local-waker")]
pub use std::task::LocalWaker as Waker;
#[cfg(not(feature = "local-waker"))]
pub use std::task::Waker;

/// The waker to park from `cx`.
pub fn of<'a>(cx: &Context<'a>) -> &'a Waker {
    #[cfg(not(feature = "local-waker"))]
    return cx.waker();
    #[cfg(feature = "local-waker")]
    return cx.local_waker();
}

/// A context for polling a child future of the task polled with
/// `parent`, so that it wakes `child` instead of the whole task.
pub(crate) fn child_context<'a>(parent: &'a Context<'_>, child: &'a Waker) -> Context<'a> {
    #[cfg(not(feature = "local-waker"))]
    {
        let _ = parent;
        Context::from_waker(child)
    }
    #[cfg(feature = "local-waker")]
    std::task::ContextBuilder::from_waker(parent.waker())
        .local_waker(child)
        .build()
}

pub(crate) use wake_set::WakeSet;

/// Tracks which of a fixed group of child futures were woken, so that a
/// combinator only re-polls those, and wakes the parent task when any of
/// them is.
#[cfg(not(feature = "local-waker"))]
mod wake_set {
    use std::sync::{Arc, Mutex};
    use std::task::{Wake, Waker};

    pub(crate) struct WakeSet(Arc<Inner>);

    struct Inner {
        state: Mutex<State>,
    }

    struct State {
        woken: Vec<usize>,
        queued: Vec<bool>,
        parent: Option<Waker>,
    }

    struct Child {
        index: usize,
        set: Arc<Inner>,
    }

    impl WakeSet {
        /// A set of `n` children, all of them initially woken.
        pub(crate) fn new(n: usize) -> Self {
            Self(Arc::new(Inner {
                state: Mutex::new(State {
                    woken: (0..n).collect(),
                    queued: vec![true; n],
                    parent: None,
                }),
            }))
        }

        pub(crate) fn child(&self, index: usize) -> Waker {
            Waker::from(Arc::new(Child {
                index,
                set: self.0.clone(),
            }))
        }

        pub(crate) fn set_parent(&self, parent: &Waker) {
            let mut s = self.0.state.lock().unwrap();
            match &mut s.parent {
                Some(w) if w.will_wake(parent) => {}
                slot => *slot = Some(parent.clone()),
            }
        }

        pub(crate) fn take_woken(&self) -> Vec<usize> {
            let mut s = self.0.state.lock().unwrap();
            let woken = std::mem::take(&mut s.woken);
            for &i in &woken {
                s.queued[i] = false;
            }
            woken
        }
    }

    impl Wake for Child {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            let parent = {
                let mut s = self.set.state.lock().unwrap();
                if s.queued[self.index] {
                    return;
                }
                s.queued[self.index] = true;
                s.woken.push(self.index);
                s.parent.clone()
            };
            if let Some(parent) = parent {
                parent.wake();
            }
        }
    }
}

#[cfg(feature = "local-waker")]
mod wake_set {
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::task::{LocalWake, LocalWaker};

    pub(crate) struct WakeSet(Rc<RefCell<State>>);

    struct State {
        woken: Vec<usize>,
        queued: Vec<bool>,
        parent: Option<LocalWaker>,
    }

    struct Child {
        index: usize,
        set: Rc<RefCell<State>>,
    }

    impl WakeSet {
        /// A set of `n` children, all of them initially woken.
        pub(crate) fn new(n: usize) -> Self {
            Self(Rc::new(RefCell::new(State {
                woken: (0..n).collect(),
                queued: vec![true; n],
                parent: None,
            })))
        }

        pub(crate) fn child(&self, index: usize) -> LocalWaker {
            LocalWaker::from(Rc::new(Child {
                index,
                set: self.0.clone(),
            }))
        }

        pub(crate) fn set_parent(&self, parent: &LocalWaker) {
            let mut s = self.0.borrow_mut();
            match &mut s.parent {
                Some(w) if w.will_wake(parent) => {}
                slot => *slot = Some(parent.clone()),
            }
        }

        pub(crate) fn take_woken(&self) -> Vec<usize> {
            let mut s = self.0.borrow_mut();
            let woken = std::mem::take(&mut s.woken);
            for &i in &woken {
                s.queued[i] = false;
            }
            woken
        }
    }

    impl LocalWake for Child {
        fn wake(self: Rc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Rc<Self>) {
            let parent = {
                let mut s = self.set.borrow_mut();
                if s.queued[self.index] {
                    return;
                }
                s.queued[self.index] = true;
                s.woken.push(self.index);
                s.parent.clone()
            };
            if let Some(parent) = parent {
                parent.wake();
            }
        }
    }
}
