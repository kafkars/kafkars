//! Exhaustive healthy-rejection classification for waiting admission.

use kafka_client_core::AdmissionRejection;

use super::{ProducerPortRejectionReason, waiting::waits_for_capacity};
use crate::{
    completion::CompletionRegistryError,
    producer::{ProducerHostInvariantError, ProducerRejectionReason, ProducerStoreError},
};

#[test]
fn only_transient_admission_capacity_enters_pending_ownership() {
    assert!(!waits_for_capacity(ProducerPortRejectionReason::Contended));
    assert!(!waits_for_capacity(
        ProducerPortRejectionReason::PendingPrecedence
    ));
    assert!(!waits_for_capacity(ProducerPortRejectionReason::Host(
        ProducerRejectionReason::HostPoisoned(ProducerHostInvariantError::MissingAdmissionIdentity,),
    )));

    for (error, expected) in [
        (CompletionRegistryError::Full, true),
        (CompletionRegistryError::UnknownCompletion, false),
        (CompletionRegistryError::DuplicatePublish, false),
        (CompletionRegistryError::NotificationBackpressure, false),
        (CompletionRegistryError::UnsettledCompletion, false),
        (CompletionRegistryError::NotifierStopped, false),
        (CompletionRegistryError::GenerationExhausted, false),
        (CompletionRegistryError::ReclaimDisconnected, false),
    ] {
        assert_eq!(
            waits_for_capacity(ProducerPortRejectionReason::Host(
                ProducerRejectionReason::Completion(error)
            )),
            expected,
            "completion classification drifted for {error:?}"
        );
    }

    for (error, expected) in [
        (ProducerStoreError::RecordCapacity, true),
        (ProducerStoreError::ByteCapacity, true),
        (ProducerStoreError::BatchCapacity, true),
        (ProducerStoreError::RetainedSizeOverflow, false),
        (ProducerStoreError::HeaderCountOutOfRange, false),
        (ProducerStoreError::PayloadIdentityExhausted, false),
        (ProducerStoreError::TopicIdentityExhausted, false),
        (ProducerStoreError::UnknownTopic, false),
        (ProducerStoreError::UnknownPayload, false),
        (ProducerStoreError::InvalidPayloadState, false),
        (ProducerStoreError::DuplicateOperation, false),
        (ProducerStoreError::DuplicatePayloadMembership, false),
        (ProducerStoreError::UnknownBatch, false),
        (ProducerStoreError::UnknownBatchMember, false),
        (ProducerStoreError::BatchRouteMismatch, false),
        (ProducerStoreError::EmptyBatch, false),
        (ProducerStoreError::BatchAlreadyMaterialized, false),
        (ProducerStoreError::StaleBatchExecution, false),
        (ProducerStoreError::PartitionOutOfRange, false),
        (ProducerStoreError::RetainedSizeMismatch, false),
        (ProducerStoreError::PayloadStillBatched, false),
    ] {
        assert_eq!(
            waits_for_capacity(ProducerPortRejectionReason::Host(
                ProducerRejectionReason::Store(error)
            )),
            expected,
            "store classification drifted for {error:?}"
        );
    }

    for (error, expected) in [
        (AdmissionRejection::Closed, false),
        (AdmissionRejection::DeadlineElapsed, false),
        (AdmissionRejection::ByteCapacity, true),
        (AdmissionRejection::CompletionCapacity, true),
        (AdmissionRejection::AccumulatorPending, true),
        (AdmissionRejection::ByteCountOverflow, false),
        (AdmissionRejection::IdentityExhausted, false),
        (AdmissionRejection::BatchIdentityExhausted, false),
        (AdmissionRejection::DeadlineOverflow, false),
    ] {
        assert_eq!(
            waits_for_capacity(ProducerPortRejectionReason::Host(
                ProducerRejectionReason::Core(error)
            )),
            expected,
            "core classification drifted for {error:?}"
        );
    }
}
