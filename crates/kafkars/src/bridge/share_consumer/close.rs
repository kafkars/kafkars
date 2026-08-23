//! Runtime-neutral facade observation of one accepted share-member close.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_engine::share::{
    ShareConsumerClose as EngineShareConsumerClose, ShareConsumerCloseAdmissionErrorKind,
    ShareConsumerCloseError, ShareConsumerCloseErrorKind,
};

use crate::{ErrorKind, KafkaError};

use super::registration::ShareConsumerEngine;

/// Private sole terminal observer over one accepted share close.
pub(crate) struct ShareConsumerClose {
    inner: EngineShareConsumerClose,
    advisory_error: Option<KafkaError>,
}

impl ShareConsumerEngine {
    #[expect(
        clippy::result_large_err,
        reason = "failed close admission returns the exact unique share owner"
    )]
    pub(crate) fn try_close(self) -> Result<ShareConsumerClose, (Self, KafkaError)> {
        let Self {
            handle,
            startup_fault,
        } = self;
        match handle.try_close() {
            Ok(close) => {
                let advisory_error = close.wake_failed().then(|| {
                    KafkaError::new(
                        ErrorKind::Internal,
                        "share close was accepted but host wakeup failed",
                    )
                });
                Ok(ShareConsumerClose {
                    inner: close,
                    advisory_error,
                })
            }
            Err(error) => {
                let semantic = translate_close_admission(error.kind());
                Err((
                    Self {
                        handle: error.into_handle(),
                        startup_fault,
                    },
                    semantic,
                ))
            }
        }
    }
}

impl ShareConsumerClose {
    pub(crate) fn advisory_error(&self) -> Option<KafkaError> {
        self.advisory_error.clone()
    }

    pub(crate) fn wait(self) -> Result<(), KafkaError> {
        self.inner.wait().map_err(translate_close_error)
    }
}

impl Future for ShareConsumerClose {
    type Output = Result<(), KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map_err(translate_close_error)
    }
}

pub(super) fn translate_close_admission(kind: ShareConsumerCloseAdmissionErrorKind) -> KafkaError {
    match kind {
        ShareConsumerCloseAdmissionErrorKind::Closed
        | ShareConsumerCloseAdmissionErrorKind::Unavailable => KafkaError::new(
            ErrorKind::State,
            "share close admission is closed or the member is unavailable",
        ),
        ShareConsumerCloseAdmissionErrorKind::Contended => KafkaError::new(
            ErrorKind::Backpressure,
            "share close owner is temporarily contended",
        )
        .with_safe_retry(),
        ShareConsumerCloseAdmissionErrorKind::Internal => {
            KafkaError::new(ErrorKind::Internal, "share close ownership is unavailable")
        }
    }
}

fn translate_close_error(error: ShareConsumerCloseError) -> KafkaError {
    match error.kind() {
        ShareConsumerCloseErrorKind::DeadlineElapsed => {
            KafkaError::new(ErrorKind::Timeout, "share close deadline elapsed")
        }
        ShareConsumerCloseErrorKind::CoordinatorUnavailable => KafkaError::new(
            ErrorKind::Routing,
            "share coordinator remained unavailable during close",
        ),
        ShareConsumerCloseErrorKind::Compatibility => KafkaError::new(
            ErrorKind::Compatibility,
            "no compatible ShareGroupHeartbeat version is available for close",
        ),
        ShareConsumerCloseErrorKind::Execution => {
            KafkaError::new(ErrorKind::Internal, "share close execution failed")
        }
        ShareConsumerCloseErrorKind::BrokerRejected => {
            KafkaError::new(ErrorKind::Broker, "Kafka rejected share-member close")
                .with_broker_code(error.broker_code())
        }
        ShareConsumerCloseErrorKind::InvalidResponse => KafkaError::new(
            ErrorKind::Internal,
            "share-member close received an invalid broker response",
        ),
        ShareConsumerCloseErrorKind::Internal => KafkaError::new(
            ErrorKind::Internal,
            "share close completion ownership is inconsistent",
        ),
    }
}

impl core::fmt::Debug for ShareConsumerClose {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("ShareConsumerClose")
            .field("advisory_error", &self.advisory_error)
            .finish_non_exhaustive()
    }
}
