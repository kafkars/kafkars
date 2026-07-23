//! Stable terminal error translation for producer sends that may wait.

use std::fmt;

use crate::{
    ProducerDeliveryError, ProducerDeliveryStatus, ProducerObserverError, ProducerRecordMetadata,
};

use super::{
    super::pending::{ProducerSendFailure, ProducerSendFailureKind, ProducerSendReadyFailure},
    error::ProducerTrySendErrorKind,
};

/// Why a producer send could not start deterministic admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProducerSendStartFailureKind {
    /// The topic name is empty.
    EmptyTopic,
    /// Automatic partition selection is not available.
    MissingExplicitPartition,
    /// The explicitly requested partition is negative.
    NegativeExplicitPartition,
    /// The original monotonic deadline cannot be represented.
    DeadlineUnrepresentable,
    /// The engine-owned boundary timestamp cannot be represented.
    TimestampUnrepresentable,
    /// The record's retained byte size cannot be represented.
    RecordSizeUnrepresentable,
    /// A bounded local operation, payload, topic, or batch identity is exhausted.
    LocalIdentityExhausted,
    /// A fatal engine invariant failed before deterministic core ownership.
    InternalInvariant,
}

/// Recordless terminal failure before deterministic producer admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProducerSendStartFailure {
    kind: ProducerSendStartFailureKind,
}

impl ProducerSendStartFailure {
    /// Creates one pre-admission failure with invariant `NotSent` certainty.
    pub const fn new(kind: ProducerSendStartFailureKind) -> Self {
        Self { kind }
    }

    /// Returns the exact pre-admission failure category.
    pub const fn kind(self) -> ProducerSendStartFailureKind {
        self.kind
    }

    /// Confirms the send never crossed transport ownership.
    pub const fn delivery_status(self) -> ProducerDeliveryStatus {
        ProducerDeliveryStatus::NotSent
    }
}

impl fmt::Display for ProducerSendStartFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "producer send could not start: {:?}", self.kind)
    }
}

impl std::error::Error for ProducerSendStartFailure {}

/// Terminal result for a producer send that may wait for local admission.
pub type ProducerSendResult = Result<ProducerRecordMetadata, ProducerSendError>;

/// Failure from producer startup, local waiting, accepted delivery, or observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProducerSendError {
    /// Work crossed core admission and then delivery observation failed.
    Delivery(ProducerDeliveryError),
    /// Work settled during ordinary local waiting and never reached transport.
    Local(ProducerSendFailure),
    /// Work could not start and never reached deterministic admission.
    Start(ProducerSendStartFailure),
    /// The pending observation cell was consumed or abandoned.
    Observer(ProducerObserverError),
}

impl ProducerSendError {
    pub(crate) const fn from_ready(failure: ProducerSendReadyFailure) -> Self {
        match failure {
            ProducerSendReadyFailure::Local(failure) => Self::Local(failure),
            ProducerSendReadyFailure::Start(failure) => Self::Start(failure),
        }
    }

    /// Returns known delivery certainty for failures before or during delivery.
    pub const fn delivery_status(self) -> Option<ProducerDeliveryStatus> {
        match self {
            Self::Delivery(ProducerDeliveryError::Failed(failure)) => {
                Some(failure.delivery_status())
            }
            Self::Local(failure) => Some(failure.delivery_status()),
            Self::Start(failure) => Some(failure.delivery_status()),
            Self::Delivery(ProducerDeliveryError::Observer(_)) | Self::Observer(_) => None,
        }
    }
}

impl fmt::Display for ProducerSendError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Delivery(error) => error.fmt(formatter),
            Self::Local(failure) => {
                write!(
                    formatter,
                    "producer send failed locally: {:?}",
                    failure.kind()
                )
            }
            Self::Start(failure) => failure.fmt(formatter),
            Self::Observer(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for ProducerSendError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Delivery(error) => Some(error),
            Self::Local(_) => None,
            Self::Start(error) => Some(error),
            Self::Observer(error) => Some(error),
        }
    }
}

pub(crate) const fn ready_from_try_send_kind(
    kind: ProducerTrySendErrorKind,
) -> ProducerSendReadyFailure {
    match kind {
        ProducerTrySendErrorKind::EmptyTopic => ProducerSendReadyFailure::Start(
            ProducerSendStartFailure::new(ProducerSendStartFailureKind::EmptyTopic),
        ),
        ProducerTrySendErrorKind::MissingExplicitPartition => ProducerSendReadyFailure::Start(
            ProducerSendStartFailure::new(ProducerSendStartFailureKind::MissingExplicitPartition),
        ),
        ProducerTrySendErrorKind::NegativeExplicitPartition => ProducerSendReadyFailure::Start(
            ProducerSendStartFailure::new(ProducerSendStartFailureKind::NegativeExplicitPartition),
        ),
        ProducerTrySendErrorKind::DeadlineUnrepresentable => ProducerSendReadyFailure::Start(
            ProducerSendStartFailure::new(ProducerSendStartFailureKind::DeadlineUnrepresentable),
        ),
        ProducerTrySendErrorKind::TimestampUnrepresentable => ProducerSendReadyFailure::Start(
            ProducerSendStartFailure::new(ProducerSendStartFailureKind::TimestampUnrepresentable),
        ),
        ProducerTrySendErrorKind::RecordSizeUnrepresentable => ProducerSendReadyFailure::Start(
            ProducerSendStartFailure::new(ProducerSendStartFailureKind::RecordSizeUnrepresentable),
        ),
        ProducerTrySendErrorKind::LocalIdentityExhausted => ProducerSendReadyFailure::Start(
            ProducerSendStartFailure::new(ProducerSendStartFailureKind::LocalIdentityExhausted),
        ),
        ProducerTrySendErrorKind::HostPoisoned | ProducerTrySendErrorKind::InternalInvariant => {
            ProducerSendReadyFailure::Start(ProducerSendStartFailure::new(
                ProducerSendStartFailureKind::InternalInvariant,
            ))
        }
        ProducerTrySendErrorKind::DeadlineElapsed => ProducerSendReadyFailure::Local(
            ProducerSendFailure::new(ProducerSendFailureKind::DeadlineElapsed),
        ),
        ProducerTrySendErrorKind::Closed => ProducerSendReadyFailure::Local(
            ProducerSendFailure::new(ProducerSendFailureKind::Closed),
        ),
        ProducerTrySendErrorKind::Contended
        | ProducerTrySendErrorKind::CompletionCapacity
        | ProducerTrySendErrorKind::RecordCapacity
        | ProducerTrySendErrorKind::ByteCapacity
        | ProducerTrySendErrorKind::BatchCapacity
        | ProducerTrySendErrorKind::AccumulatorPending => ProducerSendReadyFailure::Local(
            ProducerSendFailure::new(ProducerSendFailureKind::Backpressure),
        ),
    }
}
