//! Stable record-retaining failures for immediate producer admission.

use std::fmt;

use super::super::{
    ProducerRejectionReason, ProducerStoreError,
    ingress::{ProducerPortAdmissionError, ProducerPortPoison, ProducerPortRejectionReason},
};
use super::record::ProducerRecord;
use crate::completion::CompletionRegistryError;

/// Stable reason caller ownership did not cross immediate admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProducerTrySendErrorKind {
    /// The topic name is empty.
    EmptyTopic,
    /// Automatic partition selection is not available in this first slice.
    MissingExplicitPartition,
    /// The explicit partition is negative.
    NegativeExplicitPartition,
    /// The monotonic deadline cannot be represented.
    DeadlineUnrepresentable,
    /// The engine timestamp default cannot be represented.
    TimestampUnrepresentable,
    /// Another thread currently owns this producer shard.
    Contended,
    /// Every terminal-completion slot is retained.
    CompletionCapacity,
    /// Every retained-record slot is occupied.
    RecordCapacity,
    /// Retaining this record would exceed the application-byte bound.
    ByteCapacity,
    /// The record's retained byte size cannot be represented.
    RecordSizeUnrepresentable,
    /// Every batch slot is occupied.
    BatchCapacity,
    /// The target accumulator must make host progress before admitting.
    AccumulatorPending,
    /// The original absolute deadline elapsed before admission.
    DeadlineElapsed,
    /// Producer admission has closed.
    Closed,
    /// A bounded monotonic identity domain is exhausted.
    LocalIdentityExhausted,
    /// The producer shard has stopped after an internal invariant failure.
    HostPoisoned,
    /// A non-semantic engine mechanism violated its internal contract.
    InternalInvariant,
}

/// Immediate admission error retaining exact pre-ownership record ownership.
#[derive(Debug)]
pub struct ProducerTrySendError {
    kind: ProducerTrySendErrorKind,
    record: ProducerRecord,
    detail: Option<String>,
}

impl ProducerTrySendError {
    /// Returns the stable failure category.
    pub const fn kind(&self) -> ProducerTrySendErrorKind {
        self.kind
    }

    /// Recovers the exact record whose ownership never crossed admission.
    pub fn into_record(self) -> ProducerRecord {
        self.record
    }

    /// Returns diagnostic detail for an internal mechanism fault.
    pub fn detail(&self) -> Option<&str> {
        self.detail.as_deref()
    }

    pub(super) const fn with_record(
        kind: ProducerTrySendErrorKind,
        record: ProducerRecord,
    ) -> Self {
        Self {
            kind,
            record,
            detail: None,
        }
    }

    pub(super) fn from_port(error: ProducerPortAdmissionError) -> Self {
        match error {
            ProducerPortAdmissionError::Rejected(rejected) => {
                let kind = map_rejection(rejected.reason());
                Self::with_record(kind, ProducerRecord::from_stored(rejected.into_record()))
            }
            ProducerPortAdmissionError::Poisoned(ProducerPortPoison::BeforeAdmission {
                record,
                ..
            }) => Self::with_record(
                ProducerTrySendErrorKind::HostPoisoned,
                ProducerRecord::from_stored(record),
            ),
            ProducerPortAdmissionError::Poisoned(ProducerPortPoison::BeforeOwnership {
                error,
                record,
            }) => Self {
                kind: ProducerTrySendErrorKind::InternalInvariant,
                record: ProducerRecord::from_stored(record),
                detail: Some(error.to_string()),
            },
        }
    }
}

impl fmt::Display for ProducerTrySendError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.detail {
            Some(detail) => write!(
                formatter,
                "producer try_send failed: {:?}: {detail}",
                self.kind
            ),
            None => write!(formatter, "producer try_send failed: {:?}", self.kind),
        }
    }
}

impl std::error::Error for ProducerTrySendError {}

fn map_rejection(reason: ProducerPortRejectionReason) -> ProducerTrySendErrorKind {
    match reason {
        ProducerPortRejectionReason::Contended => ProducerTrySendErrorKind::Contended,
        ProducerPortRejectionReason::Host(reason) => map_host_rejection(reason),
    }
}

fn map_host_rejection(reason: ProducerRejectionReason) -> ProducerTrySendErrorKind {
    use kafka_client_core::AdmissionRejection;
    match reason {
        ProducerRejectionReason::Completion(CompletionRegistryError::Full)
        | ProducerRejectionReason::Core(AdmissionRejection::CompletionCapacity) => {
            ProducerTrySendErrorKind::CompletionCapacity
        }
        ProducerRejectionReason::Completion(CompletionRegistryError::NotifierStopped)
        | ProducerRejectionReason::Core(AdmissionRejection::Closed) => {
            ProducerTrySendErrorKind::Closed
        }
        ProducerRejectionReason::Store(ProducerStoreError::RecordCapacity) => {
            ProducerTrySendErrorKind::RecordCapacity
        }
        ProducerRejectionReason::Store(ProducerStoreError::ByteCapacity)
        | ProducerRejectionReason::Core(AdmissionRejection::ByteCapacity) => {
            ProducerTrySendErrorKind::ByteCapacity
        }
        ProducerRejectionReason::Store(
            ProducerStoreError::RetainedSizeOverflow | ProducerStoreError::HeaderCountOutOfRange,
        )
        | ProducerRejectionReason::Core(AdmissionRejection::ByteCountOverflow) => {
            ProducerTrySendErrorKind::RecordSizeUnrepresentable
        }
        ProducerRejectionReason::Store(ProducerStoreError::BatchCapacity) => {
            ProducerTrySendErrorKind::BatchCapacity
        }
        ProducerRejectionReason::Core(AdmissionRejection::AccumulatorPending) => {
            ProducerTrySendErrorKind::AccumulatorPending
        }
        ProducerRejectionReason::Core(AdmissionRejection::DeadlineElapsed) => {
            ProducerTrySendErrorKind::DeadlineElapsed
        }
        ProducerRejectionReason::HostPoisoned(_) => ProducerTrySendErrorKind::HostPoisoned,
        ProducerRejectionReason::Waiting(
            kafka_client_core::ProducerWaitingAdmissionError::Closed,
        ) => ProducerTrySendErrorKind::Closed,
        ProducerRejectionReason::Waiting(
            kafka_client_core::ProducerWaitingAdmissionError::DeadlineElapsed,
        ) => ProducerTrySendErrorKind::DeadlineElapsed,
        ProducerRejectionReason::Waiting(
            kafka_client_core::ProducerWaitingAdmissionError::RecordCapacity,
        ) => ProducerTrySendErrorKind::RecordCapacity,
        ProducerRejectionReason::Waiting(
            kafka_client_core::ProducerWaitingAdmissionError::ByteCapacity,
        ) => ProducerTrySendErrorKind::ByteCapacity,
        ProducerRejectionReason::Waiting(
            kafka_client_core::ProducerWaitingAdmissionError::ByteCountOverflow,
        ) => ProducerTrySendErrorKind::RecordSizeUnrepresentable,
        ProducerRejectionReason::Waiting(
            kafka_client_core::ProducerWaitingAdmissionError::IdentityExhausted,
        )
        | ProducerRejectionReason::Core(
            AdmissionRejection::IdentityExhausted | AdmissionRejection::BatchIdentityExhausted,
        )
        | ProducerRejectionReason::Store(
            ProducerStoreError::PayloadIdentityExhausted
            | ProducerStoreError::TopicIdentityExhausted,
        ) => ProducerTrySendErrorKind::LocalIdentityExhausted,
        ProducerRejectionReason::Completion(_)
        | ProducerRejectionReason::Store(_)
        | ProducerRejectionReason::Core(_) => ProducerTrySendErrorKind::InternalInvariant,
    }
}
