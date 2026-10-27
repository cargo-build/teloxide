//! Stopping asynchronous tasks, e.g., [listeners].
//!
//! [listeners]: crate::update_listeners

use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task,
};

use event_listener::{Event, EventListener};

/// Create a new token/flag pair.
#[must_use]
pub fn mk_stop_token() -> (StopToken, StopFlag) {
    let state = Arc::new(StopState { stopped: AtomicBool::new(false), event: Event::new() });
    (StopToken(state.clone()), StopFlag { state, listener: None })
}

/// A stop token which corresponds to a [`StopFlag`].
#[derive(Clone)]
pub struct StopToken(Arc<StopState>);

/// A flag which corresponds to [`StopToken`].
///
/// To know if the stop token was used you can either repeatedly call
/// [`is_stopped`] or use this type as a `Future`.
///
/// [`is_stopped`]: StopFlag::is_stopped
pub struct StopFlag {
    state: Arc<StopState>,
    listener: Option<Pin<Box<EventListener>>>,
}

struct StopState {
    stopped: AtomicBool,
    event: Event,
}

impl Clone for StopFlag {
    fn clone(&self) -> Self {
        Self { state: self.state.clone(), listener: None }
    }
}

impl StopToken {
    /// "Stops" the flag associated with this token.
    ///
    /// Note that calling this function multiple times does nothing, only the
    /// first call changes the state.
    pub fn stop(&self) {
        if !self.0.stopped.swap(true, Ordering::AcqRel) {
            self.0.event.notify(usize::MAX);
        }
    }
}

impl StopFlag {
    /// Returns true if the stop token linked to `self` was used.
    #[must_use]
    pub fn is_stopped(&self) -> bool {
        self.state.stopped.load(Ordering::Acquire)
    }
}

/// This future resolves when a stop token was used.
impl Future for StopFlag {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        let this = self.get_mut();
        if this.is_stopped() {
            return task::Poll::Ready(());
        }
        let listener = this.listener.get_or_insert_with(|| Box::pin(this.state.event.listen()));
        if this.state.stopped.load(Ordering::Acquire) {
            return task::Poll::Ready(());
        }
        listener.as_mut().poll(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::task::{waker, ArcWake};
    use std::sync::atomic::AtomicUsize;

    struct WakeCount(AtomicUsize);

    impl ArcWake for WakeCount {
        fn wake_by_ref(this: &Arc<Self>) {
            this.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn stopping_wakes_every_independently_polled_clone() {
        let (token, flag) = mk_stop_token();
        let mut flags = [flag.clone(), flag];
        let counts =
            [Arc::new(WakeCount(AtomicUsize::new(0))), Arc::new(WakeCount(AtomicUsize::new(0)))];
        let wakers = counts.each_ref().map(|count| waker(count.clone()));
        flags.iter_mut().zip(&wakers).for_each(|(flag, waker)| {
            assert!(Pin::new(flag).poll(&mut task::Context::from_waker(waker)).is_pending());
        });
        token.stop();
        counts.iter().for_each(|count| assert!(count.0.load(Ordering::Relaxed) > 0));
        flags.iter_mut().zip(&wakers).for_each(|(flag, waker)| {
            assert!(Pin::new(flag).poll(&mut task::Context::from_waker(waker)).is_ready());
        });
        assert!(token.0.stopped.load(Ordering::Acquire));
    }
}
