//! Sole public terminal observation of one accepted hosted group close.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{
    KafkaError, bridge::consumer_facade::group_consumer_close::GroupConsumerClose as BridgeClose,
};

use super::{Consumer, ConsumerCloseAdmissionError};

impl Consumer {
    /// Attempts explicit close and returns its sole terminal observer.
    ///
    /// Rejection returns this exact consumer; acceptance fences later work.
    #[expect(
        clippy::result_large_err,
        reason = "pre-admission rejection returns the exact unique consumer"
    )]
    pub fn try_close(self) -> Result<CloseConsumer, ConsumerCloseAdmissionError> {
        let Consumer {
            engine,
            group_id,
            topics,
        } = self;
        match engine.try_close() {
            Ok(close) => Ok(CloseConsumer::from_bridge(close)),
            Err((engine, error)) => Err(ConsumerCloseAdmissionError::new(
                Consumer {
                    engine,
                    group_id,
                    topics,
                },
                error,
            )),
        }
    }
}

/// Runtime-neutral terminal observer for one accepted group-consumer close.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted group close"]
pub struct CloseConsumer {
    inner: BridgeClose,
}

impl CloseConsumer {
    pub(crate) const fn from_bridge(inner: BridgeClose) -> Self {
        Self { inner }
    }

    /// Reports advisory reactor-wake degradation after close was accepted.
    pub fn advisory_error(&self) -> Option<KafkaError> {
        self.inner.advisory_error()
    }

    /// Blocks on the same bounded terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<(), KafkaError> {
        self.inner.wait()
    }
}

impl Future for CloseConsumer {
    type Output = Result<(), KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
