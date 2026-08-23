//! Named runtime-neutral observation of one accepted share acknowledgement.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use crate::{KafkaError, bridge::share_consumer::ShareConsumerAcknowledge as BridgeAcknowledge};

use super::{
    ShareAcknowledgement, ShareAcknowledgementAdmissionError, ShareAcknowledgementError,
    ShareAcknowledgementResponse, ShareConsumer,
};

impl ShareConsumer {
    /// Attempts one bounded, session-fenced share acknowledgement operation.
    ///
    /// Pre-admission rejection returns the exact acknowledgement. Acceptance
    /// already owns terminal-completion capacity and is never cancelled by
    /// dropping the returned observer.
    pub fn try_acknowledge(
        &mut self,
        acknowledgement: ShareAcknowledgement,
        timeout: Duration,
    ) -> Result<AcknowledgeShareConsumer, ShareAcknowledgementAdmissionError> {
        self.engine
            .try_acknowledge(acknowledgement.into_bridge(), timeout)
            .map(AcknowledgeShareConsumer::from_bridge)
            .map_err(|(acknowledgement, error)| {
                ShareAcknowledgementAdmissionError::new(
                    ShareAcknowledgement::from_bridge(acknowledgement),
                    error,
                )
            })
    }
}

/// Sole terminal observer for one accepted share acknowledgement.
#[derive(Debug)]
#[must_use = "dropping observation does not cancel an accepted acknowledgement"]
pub struct AcknowledgeShareConsumer {
    inner: BridgeAcknowledge,
}

impl AcknowledgeShareConsumer {
    const fn from_bridge(inner: BridgeAcknowledge) -> Self {
        Self { inner }
    }

    /// Reports advisory reactor-wake degradation after acceptance.
    pub fn advisory_error(&self) -> Option<KafkaError> {
        self.inner.advisory_error()
    }

    /// Blocks on the same bounded terminal cell used by [`Future::poll`].
    pub fn wait(self) -> Result<ShareAcknowledgementResponse, ShareAcknowledgementError> {
        self.inner
            .wait()
            .map(ShareAcknowledgementResponse::from_bridge)
            .map_err(ShareAcknowledgementError::from_bridge)
    }
}

impl Future for AcknowledgeShareConsumer {
    type Output = Result<ShareAcknowledgementResponse, ShareAcknowledgementError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context).map(|result| {
            result
                .map(ShareAcknowledgementResponse::from_bridge)
                .map_err(ShareAcknowledgementError::from_bridge)
        })
    }
}
