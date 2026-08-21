//! Runtime-neutral facade observation of one accepted hosted group close.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_engine::{
    GroupConsumerClose as EngineClose, GroupConsumerCloseError, GroupConsumerCloseErrorKind,
};

use crate::{ErrorKind, KafkaError};

/// Private sole terminal observer over one accepted group-consumer close.
pub(crate) struct GroupConsumerClose {
    inner: EngineClose,
    advisory_error: Option<KafkaError>,
}

impl GroupConsumerClose {
    pub(super) const fn new(inner: EngineClose, advisory_error: Option<KafkaError>) -> Self {
        Self {
            inner,
            advisory_error,
        }
    }

    pub(crate) fn advisory_error(&self) -> Option<KafkaError> {
        self.advisory_error.clone()
    }

    pub(crate) fn wait(self) -> Result<(), KafkaError> {
        self.inner.wait().map_err(translate_close_error)
    }
}

impl Future for GroupConsumerClose {
    type Output = Result<(), KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map_err(translate_close_error)
    }
}

fn translate_close_error(error: GroupConsumerCloseError) -> KafkaError {
    translate_close_kind(error.kind(), error.broker_code())
}

pub(super) fn translate_close_kind(
    kind: GroupConsumerCloseErrorKind,
    broker_code: Option<i16>,
) -> KafkaError {
    match kind {
        GroupConsumerCloseErrorKind::DeadlineElapsed => {
            KafkaError::new(ErrorKind::Timeout, "group close deadline elapsed")
        }
        GroupConsumerCloseErrorKind::DriverRejected => KafkaError::new(
            ErrorKind::Backpressure,
            "group close was rejected before driver admission",
        ),
        GroupConsumerCloseErrorKind::Transport => {
            KafkaError::new(ErrorKind::Transport, "group close transport failed")
        }
        GroupConsumerCloseErrorKind::Authentication => KafkaError::new(
            ErrorKind::Access,
            "Kafka authentication rejected group close",
        ),
        GroupConsumerCloseErrorKind::BrokerRejected => {
            KafkaError::new(ErrorKind::Broker, "Kafka rejected LeaveGroup")
                .with_broker_code(broker_code)
        }
        GroupConsumerCloseErrorKind::Compatibility => KafkaError::new(
            ErrorKind::Compatibility,
            "no compatible LeaveGroup protocol version is available",
        ),
        GroupConsumerCloseErrorKind::InvalidResponse
        | GroupConsumerCloseErrorKind::ResponseTooLarge => KafkaError::new(
            ErrorKind::Internal,
            "LeaveGroup returned an invalid or over-budget response",
        ),
        GroupConsumerCloseErrorKind::HostUnavailable => {
            KafkaError::new(ErrorKind::Internal, "group close host is unavailable")
        }
        GroupConsumerCloseErrorKind::InternalInvariant => KafkaError::new(
            ErrorKind::Internal,
            "group close terminal ownership is inconsistent",
        ),
    }
}

impl std::fmt::Debug for GroupConsumerClose {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GroupConsumerClose")
            .field("advisory_error", &self.advisory_error)
            .finish_non_exhaustive()
    }
}
