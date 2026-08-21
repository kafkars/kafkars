//! Private runtime-neutral translation over the engine receive operation.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_engine::AssignedConsumerRecv as EngineRecv;

use crate::KafkaError;

use super::{batch::AssignedConsumerBatch, recv_result::translate_assigned_consumer_recv};

/// Private named receive retaining the engine's unique consumer borrow.
pub(crate) struct AssignedConsumerRecv<'consumer> {
    inner: EngineRecv<'consumer>,
}

impl<'consumer> AssignedConsumerRecv<'consumer> {
    pub(super) const fn from_engine(inner: EngineRecv<'consumer>) -> Self {
        Self { inner }
    }

    pub(crate) fn wait(self) -> Result<Option<AssignedConsumerBatch>, KafkaError> {
        self.inner
            .wait()
            .map(|batch| batch.map(AssignedConsumerBatch::from_engine))
            .map_err(translate_assigned_consumer_recv)
    }
}

impl Future for AssignedConsumerRecv<'_> {
    type Output = Result<Option<AssignedConsumerBatch>, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context).map(|result| {
            result
                .map(|batch| batch.map(AssignedConsumerBatch::from_engine))
                .map_err(translate_assigned_consumer_recv)
        })
    }
}

impl std::fmt::Debug for AssignedConsumerRecv<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AssignedConsumerRecv")
            .finish_non_exhaustive()
    }
}
