//! Exhaustive translation of host admission rejection into promotion action.

use kafka_client_core::AdmissionRejection;

use crate::{
    ProducerSendStartFailure, ProducerSendStartFailureKind,
    completion::CompletionRegistryError,
    producer::{
        ProducerRejectionReason, ProducerStoreError, pending::ProducerSendFailure,
        pending::ProducerSendFailureKind,
    },
};

use super::promotion_error::PendingPromotionInvariant;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RejectionAction {
    Restore,
    Local(ProducerSendFailure),
    Start {
        failure: ProducerSendStartFailure,
        invariant: Option<PendingPromotionInvariant>,
    },
    Fatal(PendingPromotionInvariant),
}

pub(super) const fn classify_rejection(reason: ProducerRejectionReason) -> RejectionAction {
    match reason {
        ProducerRejectionReason::Completion(CompletionRegistryError::Full)
        | ProducerRejectionReason::Store(
            ProducerStoreError::RecordCapacity | ProducerStoreError::ByteCapacity,
        )
        | ProducerRejectionReason::Core(
            AdmissionRejection::CompletionCapacity
            | AdmissionRejection::ByteCapacity
            | AdmissionRejection::AccumulatorPending,
        ) => RejectionAction::Restore,
        ProducerRejectionReason::Core(AdmissionRejection::DeadlineElapsed) => {
            RejectionAction::Local(ProducerSendFailure::new(
                ProducerSendFailureKind::DeadlineElapsed,
            ))
        }
        ProducerRejectionReason::Store(
            ProducerStoreError::RetainedSizeOverflow | ProducerStoreError::HeaderCountOutOfRange,
        )
        | ProducerRejectionReason::Core(AdmissionRejection::ByteCountOverflow) => {
            start(ProducerSendStartFailureKind::RecordSizeUnrepresentable)
        }
        ProducerRejectionReason::Store(
            ProducerStoreError::PayloadIdentityExhausted
            | ProducerStoreError::TopicIdentityExhausted,
        )
        | ProducerRejectionReason::Core(
            AdmissionRejection::IdentityExhausted | AdmissionRejection::BatchIdentityExhausted,
        ) => start(ProducerSendStartFailureKind::LocalIdentityExhausted),
        ProducerRejectionReason::Core(AdmissionRejection::DeadlineOverflow) => {
            start(ProducerSendStartFailureKind::InternalInvariant)
        }
        ProducerRejectionReason::HostPoisoned(error) => RejectionAction::Start {
            failure: ProducerSendStartFailure::new(ProducerSendStartFailureKind::InternalInvariant),
            invariant: Some(PendingPromotionInvariant::Host(error)),
        },
        ProducerRejectionReason::Completion(
            CompletionRegistryError::UnknownCompletion
            | CompletionRegistryError::DuplicatePublish
            | CompletionRegistryError::NotificationBackpressure
            | CompletionRegistryError::UnsettledCompletion
            | CompletionRegistryError::NotifierStopped
            | CompletionRegistryError::GenerationExhausted
            | CompletionRegistryError::ReclaimDisconnected,
        )
        | ProducerRejectionReason::Store(
            ProducerStoreError::BatchCapacity
            | ProducerStoreError::UnknownTopic
            | ProducerStoreError::UnknownPayload
            | ProducerStoreError::InvalidPayloadState
            | ProducerStoreError::DuplicateOperation
            | ProducerStoreError::DuplicatePayloadMembership
            | ProducerStoreError::UnknownBatch
            | ProducerStoreError::UnknownBatchMember
            | ProducerStoreError::BatchRouteMismatch
            | ProducerStoreError::EmptyBatch
            | ProducerStoreError::BatchAlreadyMaterialized
            | ProducerStoreError::StaleBatchExecution
            | ProducerStoreError::PartitionOutOfRange
            | ProducerStoreError::RetainedSizeMismatch
            | ProducerStoreError::PayloadStillBatched,
        )
        | ProducerRejectionReason::Core(AdmissionRejection::Closed) => {
            RejectionAction::Fatal(PendingPromotionInvariant::UnexpectedRejection(reason))
        }
    }
}

const fn start(kind: ProducerSendStartFailureKind) -> RejectionAction {
    RejectionAction::Start {
        failure: ProducerSendStartFailure::new(kind),
        invariant: None,
    }
}
