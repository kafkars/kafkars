//! Runtime-neutral operation handles shared by asynchronous and blocking examples.

use std::future::{Future, Ready};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

/// Prototype operation that is immediately ready in the fake facade.
///
/// The production type will own cancellation, deadline, and completion-ledger
/// identity. This placeholder exists solely to compile API examples.
#[derive(Debug)]
pub struct Operation<T> {
    inner: Ready<T>,
    deadline_override: Option<Duration>,
}

impl<T> Operation<T> {
    pub(crate) fn ready(value: T) -> Self {
        Self {
            inner: std::future::ready(value),
            deadline_override: None,
        }
    }

    /// Overrides the configured operation deadline relative to creation time.
    pub fn deadline_after(mut self, duration: Duration) -> Self {
        self.deadline_override = Some(duration);
        self
    }

    /// Returns the prototype deadline override.
    pub const fn deadline_override(&self) -> Option<Duration> {
        self.deadline_override
    }

    /// Blocks for the operation result.
    ///
    /// The prototype is already ready. The production implementation will wait
    /// on the same completion cell used by `Future` polling.
    pub fn wait(self) -> T {
        self.inner.into_inner()
    }
}

impl<T> Future for Operation<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
