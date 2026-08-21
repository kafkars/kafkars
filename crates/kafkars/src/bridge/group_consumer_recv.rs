//! Private runtime-neutral translation over hosted group batch observation.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_engine::GroupConsumerRecv as EngineRecv;

use crate::KafkaError;

use super::group_consumer_batch::GroupConsumerBatch;
use super::group_consumer_recv_result::{internal_recv_error, translate_group_consumer_recv};

/// Private named receive retaining the engine's unique group-consumer borrow.
pub(crate) struct GroupConsumerRecv<'consumer> {
    inner: GroupConsumerRecvInner<'consumer>,
}

enum GroupConsumerRecvInner<'consumer> {
    Engine(EngineRecv<'consumer>),
    Rejected(Option<KafkaError>),
}

impl<'consumer> GroupConsumerRecv<'consumer> {
    pub(super) const fn from_engine(inner: EngineRecv<'consumer>) -> Self {
        Self {
            inner: GroupConsumerRecvInner::Engine(inner),
        }
    }

    pub(super) fn rejected(error: KafkaError) -> Self {
        Self {
            inner: GroupConsumerRecvInner::Rejected(Some(error)),
        }
    }

    pub(crate) fn wait(self) -> Result<Option<GroupConsumerBatch>, KafkaError> {
        match self.inner {
            GroupConsumerRecvInner::Engine(inner) => inner
                .wait()
                .map(|batch| batch.map(GroupConsumerBatch::from_engine))
                .map_err(translate_group_consumer_recv),
            GroupConsumerRecvInner::Rejected(mut error) => Err(error.take().unwrap_or_else(|| {
                internal_recv_error("group receive error was already observed")
            })),
        }
    }
}

impl Future for GroupConsumerRecv<'_> {
    type Output = Result<Option<GroupConsumerBatch>, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match &mut this.inner {
            GroupConsumerRecvInner::Engine(inner) => Pin::new(inner).poll(context).map(|result| {
                result
                    .map(|batch| batch.map(GroupConsumerBatch::from_engine))
                    .map_err(translate_group_consumer_recv)
            }),
            GroupConsumerRecvInner::Rejected(error) => {
                Poll::Ready(Err(error.take().unwrap_or_else(|| {
                    internal_recv_error("group receive error was already observed")
                })))
            }
        }
    }
}

impl std::fmt::Debug for GroupConsumerRecv<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GroupConsumerRecv")
            .finish_non_exhaustive()
    }
}
