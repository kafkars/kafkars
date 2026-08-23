//! Private runtime-neutral translation over hosted share batch observation.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_engine::share::ShareConsumerRecv as EngineRecv;

use crate::KafkaError;

use super::{
    ShareConsumerBatch,
    recv_result::{internal_recv_error, translate_share_consumer_recv},
    registration::ShareConsumerEngine,
};

/// Private named receive retaining the engine's unique share-consumer borrow.
pub(crate) struct ShareConsumerRecv<'consumer> {
    inner: ShareConsumerRecvInner<'consumer>,
}

enum ShareConsumerRecvInner<'consumer> {
    Engine(EngineRecv<'consumer>),
    Rejected(Option<KafkaError>),
}

impl<'consumer> ShareConsumerRecv<'consumer> {
    const fn from_engine(inner: EngineRecv<'consumer>) -> Self {
        Self {
            inner: ShareConsumerRecvInner::Engine(inner),
        }
    }

    fn rejected(error: KafkaError) -> Self {
        Self {
            inner: ShareConsumerRecvInner::Rejected(Some(error)),
        }
    }

    pub(crate) fn wait(self) -> Result<Option<ShareConsumerBatch>, KafkaError> {
        match self.inner {
            ShareConsumerRecvInner::Engine(inner) => inner
                .wait()
                .map(|batch| batch.map(ShareConsumerBatch::from_engine))
                .map_err(translate_share_consumer_recv),
            ShareConsumerRecvInner::Rejected(mut error) => Err(error.take().unwrap_or_else(|| {
                internal_recv_error("share receive error was already observed")
            })),
        }
    }
}

impl Future for ShareConsumerRecv<'_> {
    type Output = Result<Option<ShareConsumerBatch>, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match &mut this.inner {
            ShareConsumerRecvInner::Engine(inner) => Pin::new(inner).poll(context).map(|result| {
                result
                    .map(|batch| batch.map(ShareConsumerBatch::from_engine))
                    .map_err(translate_share_consumer_recv)
            }),
            ShareConsumerRecvInner::Rejected(error) => {
                Poll::Ready(Err(error.take().unwrap_or_else(|| {
                    internal_recv_error("share receive error was already observed")
                })))
            }
        }
    }
}

impl std::fmt::Debug for ShareConsumerRecv<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ShareConsumerRecv")
            .finish_non_exhaustive()
    }
}

impl ShareConsumerEngine {
    /// Observes one already-authorized delivery without starting `ShareFetch`.
    pub(crate) fn recv(&mut self) -> ShareConsumerRecv<'_> {
        match self.startup_fault() {
            Some(error) => ShareConsumerRecv::rejected(error),
            None => ShareConsumerRecv::from_engine(self.handle.recv()),
        }
    }
}
