//! Runtime-neutral bounded wait shared by opt-in cluster workflows.

use std::{
    future::Future,
    io,
    task::{Context, Poll, Waker},
    thread,
    time::{Duration, Instant},
};

pub(crate) const OPERATION_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) fn wait_within<F: Future>(future: F, phase: &str) -> Result<F::Output, io::Error> {
    let deadline = Instant::now() + OPERATION_TIMEOUT;
    let mut future = Box::pin(future);
    let mut context = Context::from_waker(Waker::noop());
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return Ok(output),
            Poll::Pending if Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(2));
            }
            Poll::Pending => {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("{phase} did not complete within {OPERATION_TIMEOUT:?}"),
                ));
            }
        }
    }
}
