//! Bounded runtime-neutral polling for public admin operation observers.

use std::{
    future::Future,
    task::{Context, Poll, Waker},
    thread,
    time::{Duration, Instant},
};

pub(crate) fn wait_within<F: Future>(future: F, phase: &str) -> F::Output {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut future = Box::pin(future);
    let mut context = Context::from_waker(Waker::noop());
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending if Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(2));
            }
            Poll::Pending => panic!("{phase} did not complete within ten seconds"),
        }
    }
}
