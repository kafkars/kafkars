//! Exhaustive semantic classification scenarios for dormant promotion.

use kafka_client_core::AdmissionRejection;

use crate::{
    ProducerSendStartFailureKind,
    completion::CompletionRegistryError,
    producer::{
        ProducerHostInvariantError, ProducerRejectionReason, ProducerStoreError,
        pending::ProducerSendFailureKind,
    },
};

use super::{
    promotion_error::PendingPromotionInvariant,
    promotion_rejection::{RejectionAction, classify_rejection},
};

#[test]
fn only_current_pre_core_capacity_rejections_restore_fifo_ownership() {
    let transient = [
        ProducerRejectionReason::Completion(CompletionRegistryError::Full),
        ProducerRejectionReason::Store(ProducerStoreError::RecordCapacity),
        ProducerRejectionReason::Store(ProducerStoreError::ByteCapacity),
        ProducerRejectionReason::Core(AdmissionRejection::CompletionCapacity),
        ProducerRejectionReason::Core(AdmissionRejection::ByteCapacity),
        ProducerRejectionReason::Core(AdmissionRejection::AccumulatorPending),
    ];

    for reason in transient {
        assert_eq!(classify_rejection(reason), RejectionAction::Restore);
    }
    assert!(matches!(
        classify_rejection(ProducerRejectionReason::Store(
            ProducerStoreError::BatchCapacity
        )),
        RejectionAction::Fatal(PendingPromotionInvariant::UnexpectedRejection(_))
    ));
}

#[test]
fn elapsed_deadline_is_the_only_ordinary_local_promotion_failure() {
    let action = classify_rejection(ProducerRejectionReason::Core(
        AdmissionRejection::DeadlineElapsed,
    ));
    let RejectionAction::Local(failure) = action else {
        panic!("elapsed deadline should settle locally")
    };
    assert_eq!(failure.kind(), ProducerSendFailureKind::DeadlineElapsed);
}

#[test]
fn representability_and_identity_rejections_keep_start_failure_precision() {
    assert_start(
        ProducerRejectionReason::Store(ProducerStoreError::HeaderCountOutOfRange),
        ProducerSendStartFailureKind::RecordSizeUnrepresentable,
    );
    assert_start(
        ProducerRejectionReason::Core(AdmissionRejection::ByteCountOverflow),
        ProducerSendStartFailureKind::RecordSizeUnrepresentable,
    );
    assert_start(
        ProducerRejectionReason::Store(ProducerStoreError::PayloadIdentityExhausted),
        ProducerSendStartFailureKind::LocalIdentityExhausted,
    );
    assert_start(
        ProducerRejectionReason::Core(AdmissionRejection::BatchIdentityExhausted),
        ProducerSendStartFailureKind::LocalIdentityExhausted,
    );
    assert_start(
        ProducerRejectionReason::Core(AdmissionRejection::DeadlineOverflow),
        ProducerSendStartFailureKind::InternalInvariant,
    );
}

#[test]
fn poisoned_or_stopped_owners_cannot_be_relabelled_as_capacity() {
    let host = ProducerHostInvariantError::MissingAdmissionIdentity;
    let action = classify_rejection(ProducerRejectionReason::HostPoisoned(host));
    let RejectionAction::Start { failure, invariant } = action else {
        panic!("host poison should settle the unadmitted send and retain its diagnostic")
    };
    assert_eq!(
        failure.kind(),
        ProducerSendStartFailureKind::InternalInvariant
    );
    assert_eq!(invariant, Some(PendingPromotionInvariant::Host(host)));

    for reason in [
        ProducerRejectionReason::Completion(CompletionRegistryError::NotifierStopped),
        ProducerRejectionReason::Core(AdmissionRejection::Closed),
        ProducerRejectionReason::Store(ProducerStoreError::UnknownPayload),
    ] {
        assert!(matches!(
            classify_rejection(reason),
            RejectionAction::Fatal(PendingPromotionInvariant::UnexpectedRejection(_))
        ));
    }
}

fn assert_start(reason: ProducerRejectionReason, expected: ProducerSendStartFailureKind) {
    let RejectionAction::Start { failure, invariant } = classify_rejection(reason) else {
        panic!("rejection should become a precise start failure")
    };
    assert_eq!(failure.kind(), expected);
    assert_eq!(invariant, None);
}
