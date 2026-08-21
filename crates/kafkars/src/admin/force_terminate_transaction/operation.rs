//! Named scalar observer over one singleton producer-fencing operation.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use super::super::{FenceProducers, FenceProducersResult};
use crate::{DeliveryStatus, ErrorKind, KafkaError};

/// Sole runtime-neutral observer for one force-terminated transaction.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct ForceTerminateTransaction {
    inner: FenceProducers,
}

impl ForceTerminateTransaction {
    pub(crate) const fn from_fence_producers(inner: FenceProducers) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal result observed by [`Future::poll`].
    pub fn wait(self) -> Result<(), KafkaError> {
        translate_fence_result(self.inner.wait())
    }
}

impl Future for ForceTerminateTransaction {
    type Output = Result<(), KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => Poll::Ready(translate_fence_result(result)),
        }
    }
}

pub(super) fn translate_fence_result(
    result: Result<FenceProducersResult, KafkaError>,
) -> Result<(), KafkaError> {
    let entries = result?.into_entries();
    if entries.len() != 1 {
        return Err(invalid_singleton_result());
    }
    let Some((_transactional_id, outcome)) = entries.into_iter().next() else {
        return Err(invalid_singleton_result());
    };
    outcome.map(|_identity| ())
}

fn invalid_singleton_result() -> KafkaError {
    KafkaError::new(
        ErrorKind::Internal,
        "ForceTerminateTransaction received a non-singleton producer-fencing terminal",
    )
    .with_delivery_status(DeliveryStatus::PossiblySent)
}
