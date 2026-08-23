//! Sole public terminal observation of one accepted share-member close.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::share_consumer::ShareConsumerClose as BridgeClose};

use super::{ShareConsumer, ShareConsumerCloseAdmissionError};

impl ShareConsumer {
    /// Attempts graceful close and returns its sole terminal observer.
    ///
    /// Rejection returns this exact consumer; acceptance fences later work.
    #[expect(
        clippy::result_large_err,
        reason = "pre-admission rejection returns the exact unique share consumer"
    )]
    pub fn try_close(self) -> Result<CloseShareConsumer, ShareConsumerCloseAdmissionError> {
        let ShareConsumer {
            engine,
            group_id,
            rack,
            topics,
        } = self;
        match engine.try_close() {
            Ok(close) => Ok(CloseShareConsumer::from_bridge(close)),
            Err((engine, error)) => Err(ShareConsumerCloseAdmissionError::new(
                ShareConsumer {
                    engine,
                    group_id,
                    rack,
                    topics,
                },
                error,
            )),
        }
    }
}

/// Runtime-neutral terminal observer for one accepted share-member close.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted share close"]
pub struct CloseShareConsumer {
    inner: BridgeClose,
}

impl CloseShareConsumer {
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

impl Future for CloseShareConsumer {
    type Output = Result<(), KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
