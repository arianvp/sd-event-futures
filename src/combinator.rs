//! Future combinators: [`race`], [`race_all`], [`join`], [`join_all`] and
//! [`select!`](crate::select).

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::waker::{self, WakeSet, Waker};

pub enum Either<A, B> {
    Left(A),
    Right(B),
}

// =============================================================================
// race
// =============================================================================

/// Future returned by [`race`].
pub struct Race<A, B> {
    a: A,
    b: B,
}

impl<A, B> Race<A, B> {
    fn project(self: Pin<&mut Self>) -> (Pin<&mut A>, Pin<&mut B>) {
        // SAFETY: the fields are pinned whenever the race is: nothing moves
        // them out, there is no `Drop` impl, and `Race` is only `Unpin` when
        // both of them are.
        unsafe {
            let this = self.get_unchecked_mut();
            (
                Pin::new_unchecked(&mut this.a),
                Pin::new_unchecked(&mut this.b),
            )
        }
    }
}

impl<A: Future, B: Future> Future for Race<A, B> {
    type Output = Either<A::Output, B::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let (a, b) = self.project();
        if let Poll::Ready(v) = a.poll(cx) {
            return Poll::Ready(Either::Left(v));
        }
        b.poll(cx).map(Either::Right)
    }
}

/// Poll `a` and `b` concurrently and resolve with whichever finishes
/// first (`a` wins ties). The loser is dropped, cancelling it.
pub fn race<A: Future, B: Future>(a: A, b: B) -> Race<A, B> {
    Race { a, b }
}

/// Future returned by [`race_all`].
pub struct RaceAll<F> {
    futures: Pin<Box<[F]>>,
    wakers: Box<[Waker]>,
    woken: WakeSet,
}

impl<F: Future> Future for RaceAll<F> {
    type Output = (usize, F::Output);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.woken.set_parent(waker::of(cx));
        for i in this.woken.take_woken() {
            let mut child_cx = waker::child_context(cx, &this.wakers[i]);
            if let Poll::Ready(v) = pinned(this.futures.as_mut(), i).poll(&mut child_cx) {
                return Poll::Ready((i, v));
            }
        }
        Poll::Pending
    }
}

/// Poll all `futures` concurrently and resolve with the index and output
/// of the first to finish. Dropping the result cancels the rest. Never
/// resolves if `futures` is empty.
///
/// Each future gets a waker of its own, so a wake only re-polls the future
/// that woke, however many there are.
pub fn race_all<I>(futures: I) -> RaceAll<I::Item>
where
    I: IntoIterator,
    I::Item: Future,
{
    let futures: Box<[I::Item]> = futures.into_iter().collect();
    let woken = WakeSet::new(futures.len());
    RaceAll {
        wakers: (0..futures.len()).map(|i| woken.child(i)).collect(),
        futures: Box::into_pin(futures),
        woken,
    }
}

// =============================================================================
// join
// =============================================================================

/// A future, then its output: what [`join`] and [`join_all`] hold for each
/// of their futures.
enum MaybeDone<F: Future> {
    Pending(F),
    Done(F::Output),
    Taken,
}

impl<F: Future> MaybeDone<F> {
    fn is_pending(&self) -> bool {
        matches!(self, MaybeDone::Pending(_))
    }

    /// Poll the future if it is still pending; true once it is done.
    fn poll_done(self: Pin<&mut Self>, cx: &mut Context<'_>) -> bool {
        // SAFETY: the future is never moved out: once finished, it is
        // dropped in place by overwriting it with its output.
        let this = unsafe { self.get_unchecked_mut() };
        if let MaybeDone::Pending(future) = this {
            // SAFETY: as above, the future stays where it is.
            match unsafe { Pin::new_unchecked(future) }.poll(cx) {
                Poll::Ready(v) => *this = MaybeDone::Done(v),
                Poll::Pending => return false,
            }
        }
        true
    }

    /// # Panics
    ///
    /// Panics unless the future is done and its output not yet taken.
    fn take(self: Pin<&mut Self>) -> F::Output {
        // SAFETY: only the output moves, and it was never pinned.
        match std::mem::replace(unsafe { self.get_unchecked_mut() }, MaybeDone::Taken) {
            MaybeDone::Done(v) => v,
            _ => panic!("join polled after completion"),
        }
    }
}

/// Future returned by [`join`].
pub struct Join<A: Future, B: Future> {
    a: MaybeDone<A>,
    b: MaybeDone<B>,
}

impl<A: Future, B: Future> Join<A, B> {
    fn project(self: Pin<&mut Self>) -> (Pin<&mut MaybeDone<A>>, Pin<&mut MaybeDone<B>>) {
        // SAFETY: as for `Race::project`.
        unsafe {
            let this = self.get_unchecked_mut();
            (
                Pin::new_unchecked(&mut this.a),
                Pin::new_unchecked(&mut this.b),
            )
        }
    }
}

impl<A: Future, B: Future> Future for Join<A, B> {
    type Output = (A::Output, B::Output);

    /// # Panics
    ///
    /// Panics if polled again after it returned `Ready`.
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let (mut a, mut b) = self.project();
        // `&`, not `&&`: both make progress on every poll.
        if a.as_mut().poll_done(cx) & b.as_mut().poll_done(cx) {
            Poll::Ready((a.take(), b.take()))
        } else {
            Poll::Pending
        }
    }
}

/// Poll `a` and `b` concurrently and resolve with both outputs once both
/// are done.
pub fn join<A: Future, B: Future>(a: A, b: B) -> Join<A, B> {
    Join {
        a: MaybeDone::Pending(a),
        b: MaybeDone::Pending(b),
    }
}

/// Future returned by [`join_all`].
pub struct JoinAll<F: Future> {
    slots: Pin<Box<[MaybeDone<F>]>>,
    wakers: Box<[Waker]>,
    woken: WakeSet,
    pending: usize,
}

impl<F: Future> Future for JoinAll<F> {
    type Output = Vec<F::Output>;

    /// # Panics
    ///
    /// Panics if polled again after it returned `Ready`.
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.woken.set_parent(waker::of(cx));
        for i in this.woken.take_woken() {
            let mut slot = pinned(this.slots.as_mut(), i);
            let mut child_cx = waker::child_context(cx, &this.wakers[i]);
            if slot.is_pending() && slot.as_mut().poll_done(&mut child_cx) {
                this.pending -= 1;
            }
        }
        if this.pending > 0 {
            return Poll::Pending;
        }
        let outputs = (0..this.slots.len())
            .map(|i| pinned(this.slots.as_mut(), i).take())
            .collect();
        Poll::Ready(outputs)
    }
}

/// Poll all `futures` concurrently and resolve with their outputs, in the
/// order the futures were given.
///
/// Each future gets a waker of its own, so a wake only re-polls the future
/// that woke, however many there are.
pub fn join_all<I>(futures: I) -> JoinAll<I::Item>
where
    I: IntoIterator,
    I::Item: Future,
{
    let slots: Box<[MaybeDone<I::Item>]> = futures.into_iter().map(MaybeDone::Pending).collect();
    let woken = WakeSet::new(slots.len());
    JoinAll {
        wakers: (0..slots.len()).map(|i| woken.child(i)).collect(),
        pending: slots.len(),
        slots: Box::into_pin(slots),
        woken,
    }
}

/// Element `i` of a pinned slice, pinned too.
fn pinned<T>(slice: Pin<&mut [T]>, i: usize) -> Pin<&mut T> {
    // SAFETY: the elements of a pinned slice can't be moved either: a
    // slice can't grow, and nothing swaps elements out through the pin.
    unsafe { slice.map_unchecked_mut(|s| &mut s[i]) }
}

// =============================================================================
// select!
// =============================================================================

/// Wait for the first of several futures to complete, then run that
/// branch's code with its output:
///
/// ```ignore
/// loop {
///     select! {
///         _ = sigterm.recv() => break,
///         _ = sighup.recv() => reload(),
///         accepted = listener.read_with(|l| l.accept()) => serve(accepted?),
///     }
/// }
/// ```
///
/// - Branches are polled in the order written, so when several are ready
///   the first one wins.
/// - The other futures are dropped, cancelling them, before the winning
///   branch's code runs, so that code may use whatever they borrowed.
/// - Branch code runs in the enclosing function: `break`, `continue`,
///   `return`, `?` and `.await` work as they would outside the macro.
/// - Patterns must be irrefutable. Up to 8 branches.
#[macro_export]
macro_rules! select {
    ($($pat:pat = $future:expr => $body:expr),+ $(,)?) => {
        $crate::__select!(
            @branches [] [_0 _1 _2 _3 _4 _5 _6 _7] [T0 T1 T2 T3 T4 T5 T6 T7]
            $($pat = $future => $body,)+
        )
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __select {
    // Give each branch a variant and a type parameter name, one at a time.
    (@branches [$($done:tt)*] [$v:ident $($vs:ident)*] [$t:ident $($ts:ident)*]
     $pat:pat = $future:expr => $body:expr, $($rest:tt)*) => {
        $crate::__select!(
            @branches [$($done)* ($v $t ($pat) ($future) ($body))] [$($vs)*] [$($ts)*] $($rest)*
        )
    };
    (@branches [$(($v:ident $t:ident ($pat:pat) ($future:expr) ($body:expr)))*] [$($vs:ident)*] [$($ts:ident)*]) => {{
        enum Branch<$($t),*> { $($v($t)),* }
        let branch = {
            $( let mut $v = ::core::pin::pin!($future); )*
            ::core::future::poll_fn(|cx| {
                $(
                    if let ::core::task::Poll::Ready(out) = ::core::future::Future::poll($v.as_mut(), cx) {
                        return ::core::task::Poll::Ready(Branch::$v(out));
                    }
                )*
                ::core::task::Poll::Pending
            })
            .await
        };
        match branch {
            $( Branch::$v($pat) => $body, )*
        }
    }};
}
