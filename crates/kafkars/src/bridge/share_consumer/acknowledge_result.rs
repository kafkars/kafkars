//! Exhaustive stable translation of accepted share acknowledgement terminals.

use kafka_client_engine::share::{
    ShareAcknowledgeDeliveryStatus as EngineDeliveryStatus,
    ShareAcknowledgeFailure as EngineFailure, ShareAcknowledgeFailureKind as EngineFailureKind,
    ShareAcknowledgePartitionOutcome as EnginePartitionOutcome,
    ShareAcknowledgeResponse as EngineResponse,
    ShareAcknowledgementObserverError as EngineObserverError,
};

use crate::{DeliveryStatus, ErrorKind, KafkaError};

use super::ShareAcknowledgement;

/// Private generated-free response to one share acknowledgement.
#[derive(Debug)]
pub(crate) struct ShareAcknowledgementResponse {
    inner: EngineResponse,
}

impl ShareAcknowledgementResponse {
    pub(super) const fn from_engine(inner: EngineResponse) -> Self {
        Self { inner }
    }

    pub(crate) const fn throttle_time_ms(&self) -> u32 {
        self.inner.throttle_time_ms()
    }

    pub(crate) fn partitions(
        &self,
    ) -> impl ExactSizeIterator<Item = ShareAcknowledgementPartitionOutcome<'_>> {
        self.inner
            .partitions()
            .map(ShareAcknowledgementPartitionOutcome::from_engine)
    }
}

/// Private borrowed result for one acknowledged topic partition.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ShareAcknowledgementPartitionOutcome<'response> {
    inner: EnginePartitionOutcome<'response>,
}

impl<'response> ShareAcknowledgementPartitionOutcome<'response> {
    const fn from_engine(inner: EnginePartitionOutcome<'response>) -> Self {
        Self { inner }
    }

    pub(crate) const fn topic_id(self) -> [u8; 16] {
        self.inner.topic_id()
    }

    pub(crate) const fn partition(self) -> u32 {
        self.inner.partition()
    }

    pub(crate) const fn broker_code(self) -> Option<i16> {
        self.inner.broker_code()
    }

    pub(crate) fn error_message(self) -> Option<&'response [u8]> {
        self.inner.error_message()
    }

    pub(crate) const fn current_leader(self) -> Option<(i32, i32)> {
        self.inner.current_leader()
    }
}

/// Private owned top-level broker rejection details.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ShareAcknowledgementBrokerError {
    throttle_time_ms: u32,
    broker_code: i16,
    message: Option<Vec<u8>>,
}

impl ShareAcknowledgementBrokerError {
    pub(crate) const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    pub(crate) const fn broker_code(&self) -> i16 {
        self.broker_code
    }

    pub(crate) fn message(&self) -> Option<&[u8]> {
        self.message.as_deref()
    }
}

/// Private accepted-operation failure with optional exact retry ownership.
#[must_use = "share acknowledgement failure may retain exact retry ownership"]
pub(crate) struct ShareAcknowledgementError {
    acknowledgement: Option<Box<ShareAcknowledgement>>,
    error: KafkaError,
    broker: Option<ShareAcknowledgementBrokerError>,
}

impl ShareAcknowledgementError {
    pub(crate) const fn broker(&self) -> Option<&ShareAcknowledgementBrokerError> {
        self.broker.as_ref()
    }

    pub(crate) fn into_parts(self) -> (Option<ShareAcknowledgement>, KafkaError) {
        (
            self.acknowledgement.map(|acknowledgement| *acknowledgement),
            self.error,
        )
    }
}

impl std::fmt::Debug for ShareAcknowledgementError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ShareAcknowledgementError")
            .field("acknowledgement", &self.acknowledgement)
            .field("error", &self.error)
            .field("broker", &self.broker)
            .finish()
    }
}

impl std::fmt::Display for ShareAcknowledgementError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(formatter)
    }
}

impl std::error::Error for ShareAcknowledgementError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

pub(super) fn translate_failure(failure: EngineFailure) -> ShareAcknowledgementError {
    let kind = failure.kind();
    let delivery = failure.delivery_status();
    let broker = failure
        .broker()
        .map(|broker| ShareAcknowledgementBrokerError {
            throttle_time_ms: broker.throttle_time_ms(),
            broker_code: broker.broker_code(),
            message: broker.message().map(<[u8]>::to_vec),
        });
    let acknowledgement = failure.into_retry().map(ShareAcknowledgement::from_engine);
    let error = failure_error(kind, delivery, broker.as_ref(), acknowledgement.is_some());
    ShareAcknowledgementError {
        acknowledgement: acknowledgement.map(Box::new),
        error,
        broker,
    }
}

fn failure_error(
    kind: EngineFailureKind,
    delivery: EngineDeliveryStatus,
    broker: Option<&ShareAcknowledgementBrokerError>,
    has_retry: bool,
) -> KafkaError {
    let public = match kind {
        EngineFailureKind::DeadlineElapsed => ErrorKind::Timeout,
        EngineFailureKind::Compatibility => ErrorKind::Compatibility,
        EngineFailureKind::DriverRejected => ErrorKind::Backpressure,
        EngineFailureKind::Transport => ErrorKind::Transport,
        EngineFailureKind::InvalidResponse | EngineFailureKind::BrokerRejected => ErrorKind::Broker,
        EngineFailureKind::ResponseTooLarge | EngineFailureKind::Internal => ErrorKind::Internal,
    };
    let error = KafkaError::new(public, format!("share acknowledgement failed: {kind:?}"))
        .with_broker_code(broker.map(ShareAcknowledgementBrokerError::broker_code))
        .with_delivery_status(match delivery {
            EngineDeliveryStatus::NotSent => DeliveryStatus::NotSent,
            EngineDeliveryStatus::PossiblySent => DeliveryStatus::PossiblySent,
        });
    if matches!(delivery, EngineDeliveryStatus::NotSent) && has_retry {
        error.with_safe_retry()
    } else {
        error
    }
}

pub(super) fn translate_observer_error(error: EngineObserverError) -> ShareAcknowledgementError {
    let public = match error {
        EngineObserverError::AlreadyObserved => ErrorKind::State,
        EngineObserverError::Stale => ErrorKind::Internal,
    };
    ShareAcknowledgementError {
        acknowledgement: None,
        error: KafkaError::new(public, error.to_string()),
        broker: None,
    }
}
