//! Producer-send error vocabulary and certainty scenarios.

use std::error::Error;

use super::{
    ProducerSendError, ProducerSendStartFailure, ProducerSendStartFailureKind,
    ProducerTrySendErrorKind, send_error::ready_from_try_send_kind,
};
use crate::producer::pending::PendingAdmissionRejectionReason;
use crate::{ProducerDeliveryStatus, ProducerSendFailure, ProducerSendFailureKind};

#[test]
fn every_start_failure_is_recordless_and_not_sent() {
    for kind in [
        ProducerSendStartFailureKind::EmptyTopic,
        ProducerSendStartFailureKind::MissingExplicitPartition,
        ProducerSendStartFailureKind::NegativeExplicitPartition,
        ProducerSendStartFailureKind::DeadlineUnrepresentable,
        ProducerSendStartFailureKind::TimestampUnrepresentable,
        ProducerSendStartFailureKind::RecordSizeUnrepresentable,
        ProducerSendStartFailureKind::LocalIdentityExhausted,
        ProducerSendStartFailureKind::InternalInvariant,
    ] {
        let failure = ProducerSendStartFailure::new(kind);
        assert_eq!(failure.kind(), kind);
        assert_eq!(failure.delivery_status(), ProducerDeliveryStatus::NotSent);
        assert_eq!(
            ProducerSendError::Start(failure).delivery_status(),
            Some(ProducerDeliveryStatus::NotSent)
        );
        assert!(ProducerSendError::Start(failure).source().is_some());
    }
}

#[test]
fn every_try_send_kind_has_one_honest_waiting_send_classification() {
    for (kind, expected) in [
        (
            ProducerTrySendErrorKind::EmptyTopic,
            start(ProducerSendStartFailureKind::EmptyTopic),
        ),
        (
            ProducerTrySendErrorKind::MissingExplicitPartition,
            start(ProducerSendStartFailureKind::MissingExplicitPartition),
        ),
        (
            ProducerTrySendErrorKind::NegativeExplicitPartition,
            start(ProducerSendStartFailureKind::NegativeExplicitPartition),
        ),
        (
            ProducerTrySendErrorKind::DeadlineUnrepresentable,
            start(ProducerSendStartFailureKind::DeadlineUnrepresentable),
        ),
        (
            ProducerTrySendErrorKind::TimestampUnrepresentable,
            start(ProducerSendStartFailureKind::TimestampUnrepresentable),
        ),
        (
            ProducerTrySendErrorKind::RecordSizeUnrepresentable,
            start(ProducerSendStartFailureKind::RecordSizeUnrepresentable),
        ),
        (
            ProducerTrySendErrorKind::LocalIdentityExhausted,
            start(ProducerSendStartFailureKind::LocalIdentityExhausted),
        ),
        (
            ProducerTrySendErrorKind::HostPoisoned,
            start(ProducerSendStartFailureKind::InternalInvariant),
        ),
        (
            ProducerTrySendErrorKind::InternalInvariant,
            start(ProducerSendStartFailureKind::InternalInvariant),
        ),
        (
            ProducerTrySendErrorKind::DeadlineElapsed,
            local(ProducerSendFailureKind::DeadlineElapsed),
        ),
        (
            ProducerTrySendErrorKind::Closed,
            local(ProducerSendFailureKind::Closed),
        ),
        (
            ProducerTrySendErrorKind::Contended,
            local(ProducerSendFailureKind::Backpressure),
        ),
        (
            ProducerTrySendErrorKind::PendingPrecedence,
            local(ProducerSendFailureKind::Backpressure),
        ),
        (
            ProducerTrySendErrorKind::CompletionCapacity,
            local(ProducerSendFailureKind::Backpressure),
        ),
        (
            ProducerTrySendErrorKind::RecordCapacity,
            local(ProducerSendFailureKind::Backpressure),
        ),
        (
            ProducerTrySendErrorKind::ByteCapacity,
            local(ProducerSendFailureKind::Backpressure),
        ),
        (
            ProducerTrySendErrorKind::BatchCapacity,
            local(ProducerSendFailureKind::Backpressure),
        ),
        (
            ProducerTrySendErrorKind::AccumulatorPending,
            local(ProducerSendFailureKind::Backpressure),
        ),
    ] {
        assert_eq!(
            ProducerSendError::from_ready(ready_from_try_send_kind(kind)),
            expected
        );
    }
}

#[test]
fn every_pending_rejection_has_one_exact_ready_classification() {
    use super::send_error::ready_from_pending_rejection;

    for (reason, expected) in [
        (
            PendingAdmissionRejectionReason::Closed,
            local(ProducerSendFailureKind::Closed),
        ),
        (
            PendingAdmissionRejectionReason::CountCapacity,
            local(ProducerSendFailureKind::Backpressure),
        ),
        (
            PendingAdmissionRejectionReason::ByteCapacity,
            local(ProducerSendFailureKind::Backpressure),
        ),
        (
            PendingAdmissionRejectionReason::NotificationBackpressure,
            local(ProducerSendFailureKind::Backpressure),
        ),
        (
            PendingAdmissionRejectionReason::RetainedSizeOverflow,
            start(ProducerSendStartFailureKind::RecordSizeUnrepresentable),
        ),
        (
            PendingAdmissionRejectionReason::IdentityExhausted,
            start(ProducerSendStartFailureKind::LocalIdentityExhausted),
        ),
    ] {
        assert_eq!(
            ProducerSendError::from_ready(ready_from_pending_rejection(reason)),
            expected
        );
    }
}

const fn start(kind: ProducerSendStartFailureKind) -> ProducerSendError {
    ProducerSendError::Start(ProducerSendStartFailure::new(kind))
}

const fn local(kind: ProducerSendFailureKind) -> ProducerSendError {
    ProducerSendError::Local(ProducerSendFailure::new(kind))
}

#[test]
fn start_failure_display_preserves_its_distinct_category() {
    let failure =
        ProducerSendStartFailure::new(ProducerSendStartFailureKind::RecordSizeUnrepresentable);
    let rendered = ProducerSendError::Start(failure).to_string();
    assert!(rendered.contains("RecordSizeUnrepresentable"));
    assert!(!rendered.contains("Backpressure"));
}
