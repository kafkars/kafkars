//! Runtime-neutral operation handles shared by asynchronous and blocking examples.

use std::future::{Future, Ready};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

/// Provisional immediately-ready operation for inactive API domains.
///
/// The implemented producer path uses its dedicated engine-backed `Delivery`
/// observer instead. Other domains retain this compile-checked design probe
/// until their own ownership transitions are implemented.
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

    /// Returns the provisional deadline override.
    pub const fn deadline_override(&self) -> Option<Duration> {
        self.deadline_override
    }

    /// Blocks for the operation result.
    ///
    /// This provisional operation is already ready.
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
