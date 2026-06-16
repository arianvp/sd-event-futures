//! The calling thread's signal mask.

/// A signal set with every signal in it, or none.
fn signal_set(full: bool) -> libc::sigset_t {
    let mut set = std::mem::MaybeUninit::<libc::sigset_t>::uninit();
    // SAFETY: both initialize the whole set.
    unsafe {
        if full {
            libc::sigfillset(set.as_mut_ptr());
        } else {
            libc::sigemptyset(set.as_mut_ptr());
        }
        set.assume_init()
    }
}

/// Run `f` with every signal blocked in the calling thread, then restore
/// the mask. Threads inherit their creator's mask, so a thread created in
/// `f` starts with all signals blocked and never takes one meant for an
/// event loop's signalfd.
pub fn with_all_signals_blocked<R>(f: impl FnOnce() -> R) -> R {
    let all = signal_set(true);
    let mut saved = signal_set(false);
    // SAFETY: both sets are valid; this thread's mask is ours to change.
    unsafe { libc::pthread_sigmask(libc::SIG_BLOCK, &all, &mut saved) };
    struct Restore(libc::sigset_t);
    impl Drop for Restore {
        fn drop(&mut self) {
            // SAFETY: restores the mask saved above.
            unsafe { libc::pthread_sigmask(libc::SIG_SETMASK, &self.0, std::ptr::null_mut()) };
        }
    }
    let _restore = Restore(saved);
    f()
}

/// Unblock every signal in the calling thread, as a freshly forked child
/// should start out.
pub fn unblock_all_signals() {
    let none = signal_set(false);
    // SAFETY: `none` is a valid set.
    unsafe { libc::pthread_sigmask(libc::SIG_SETMASK, &none, std::ptr::null_mut()) };
}

/// Whether `signal` is blocked in the calling thread.
pub fn is_blocked(signal: libc::c_int) -> bool {
    let mut mask = signal_set(false);
    // SAFETY: a null new set only reads the current mask into `mask`.
    unsafe { libc::pthread_sigmask(libc::SIG_BLOCK, std::ptr::null(), &mut mask) };
    // SAFETY: `mask` is a valid set.
    unsafe { libc::sigismember(&mask, signal) == 1 }
}

/// Send `signal` to the calling thread.
pub fn raise(signal: libc::c_int) {
    // SAFETY: no pointers involved.
    unsafe { libc::raise(signal) };
}
