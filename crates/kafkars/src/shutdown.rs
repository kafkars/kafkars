//! Named public observation of one clone-shared terminal client shutdown.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::client_shutdown::ClientShutdown};

/// Graceful clone-shared client shutdown.
///
/// The first call permanently fences new work and starts one native shutdown
/// waiter. Later calls observe the same retained terminal result.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling client shutdown"]
pub struct Shutdown {
    inner: ClientShutdown,
}

impl Shutdown {
    pub(crate) const fn from_bridge(inner: ClientShutdown) -> Self {
        Self { inner }
    }

    /// Blocks until host cleanup and all worker termination are reported.
    pub fn wait(self) -> Result<(), KafkaError> {
        self.inner.wait()
    }
}

impl Future for Shutdown {
    type Output = Result<(), KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().inner).poll(context)
    }
}
