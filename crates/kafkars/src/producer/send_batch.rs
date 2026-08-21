//! Named aggregate observation for one prefix-admitted producer batch.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, Record, RecordMetadata, TrySendError, bridge::producer::ProducerBatch};

/// Ordered terminal deliveries plus an optional exact unaccepted suffix.
#[derive(Debug)]
pub struct SendBatchResult {
    deliveries: Vec<Result<RecordMetadata, KafkaError>>,
    rejection: Option<TrySendError<Vec<Record>>>,
}

impl SendBatchResult {
    pub(crate) const fn new(
        deliveries: Vec<Result<RecordMetadata, KafkaError>>,
        rejection: Option<TrySendError<Vec<Record>>>,
    ) -> Self {
        Self {
            deliveries,
            rejection,
        }
    }

    /// Borrows terminal outcomes for the accepted prefix in input order.
    pub fn deliveries(&self) -> &[Result<RecordMetadata, KafkaError>] {
        &self.deliveries
    }

    /// Borrows the first admission failure and exact unaccepted suffix.
    pub const fn rejection(&self) -> Option<&TrySendError<Vec<Record>>> {
        self.rejection.as_ref()
    }

    /// Transfers the ordered delivery outcomes and optional rejected suffix.
    #[expect(
        clippy::type_complexity,
        reason = "the ownership tuple exactly returns accepted outcomes and the unaccepted record suffix"
    )]
    pub fn into_parts(
        self,
    ) -> (
        Vec<Result<RecordMetadata, KafkaError>>,
        Option<TrySendError<Vec<Record>>>,
    ) {
        (self.deliveries, self.rejection)
    }
}

/// Runtime-neutral observer for all records accepted by one batch call.
///
/// Dropping this value abandons observation only. Every admitted record
/// continues to one terminal result in the engine.
#[derive(Debug)]
#[must_use = "dropping abandons accepted batch observation without cancelling producer work"]
pub struct SendBatch {
    inner: ProducerBatch,
}

impl SendBatch {
    pub(crate) const fn from_bridge(inner: ProducerBatch) -> Self {
        Self { inner }
    }

    /// Blocks on the same accepted-prefix terminals used by `Future::poll`.
    pub fn wait(self) -> SendBatchResult {
        self.inner.wait()
    }
}

impl Future for SendBatch {
    type Output = SendBatchResult;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().inner).poll(context)
    }
}
