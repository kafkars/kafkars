//! Exact bounded eligibility and terminal fallback for coordinator commit replacement.

use kafka_client_core::{DeliveryStatus, GroupOffsetCommitInput, GroupOffsetCommitPartitionResult};
use kafka_driver::{ApiVersion, CallFailure, Delivery, RequestError};
use kafka_wire::{
    OffsetCommitResponse,
    offset_commit_response::{OffsetCommitResponsePartition, OffsetCommitResponseTopic},
};

use super::{GroupOffsetCommitRetryCandidate, candidate::replacement_admission_terminal};
use crate::driver::rpc::group_offset_commit_calls_test::prepared;

#[test]
fn exact_initial_coordinator_rejection_retains_one_same_deadline_candidate() {
    for code in [15, 16] {
        let original = prepared(u64::try_from(code).unwrap_or_else(|_| panic!("positive code")));
        let operation_id = original.operation_id();
        let deadline = original.operation_deadline();
        let candidate = GroupOffsetCommitRetryCandidate::try_new(
            original,
            Some(ApiVersion::new(9)),
            Ok(response(code)),
        )
        .unwrap_or_else(|_| panic!("exact coordinator response is retryable"));
        assert_eq!(candidate.operation_id(), operation_id);
        let replacement = candidate.into_prepared();
        assert_eq!(replacement.operation_id(), operation_id);
        assert_eq!(replacement.operation_deadline(), deadline);
        assert_eq!(replacement.entries_capacity(), 1);
        assert_eq!(replacement.outcomes_capacity(), 1);
    }
}

#[test]
fn successful_or_incompatible_response_cannot_become_a_replacement() {
    for (version, code) in [(9, 0), (1, 16)] {
        let candidate = GroupOffsetCommitRetryCandidate::try_new(
            prepared(21),
            Some(ApiVersion::new(version)),
            Ok(response(code)),
        );
        let Err((prepared, Ok(response))) = candidate else {
            panic!("noneligible response must remain terminal");
        };
        assert_eq!(
            prepared.operation_id(),
            kafka_client_core::OperationId::from_raw(21)
        );
        assert_eq!(response.topics[0].partitions[0].error_code, code);
    }
}

#[test]
fn retry_candidate_can_restore_the_exact_first_broker_terminal() {
    let candidate = GroupOffsetCommitRetryCandidate::try_new(
        prepared(31),
        Some(ApiVersion::new(9)),
        Ok(response(16)),
    )
    .unwrap_or_else(|_| panic!("candidate"));
    let GroupOffsetCommitInput::BrokerResponded { outcomes, .. } = candidate.into_terminal() else {
        panic!("broker terminal");
    };
    let GroupOffsetCommitPartitionResult::Rejected(error) = outcomes[0].result() else {
        panic!("rejection");
    };
    assert_eq!(error.code(), 16);
}

#[test]
fn replacement_local_failures_never_claim_ambiguous_delivery() {
    assert_eq!(
        replacement_admission_terminal(true),
        GroupOffsetCommitInput::ExecutionUnavailable
    );
    assert_eq!(
        replacement_admission_terminal(false),
        GroupOffsetCommitInput::TransportFailed {
            delivery: DeliveryStatus::NotSent
        }
    );
}

#[test]
fn known_unsent_route_loss_preserves_the_exact_checkpoint_and_deadline() {
    for version in [None, Some(ApiVersion::new(9))] {
        let original = prepared(41);
        let deadline = original.operation_deadline();
        let request = crate::protocol::consumer::group_offset_commit_request(&original);
        let candidate = GroupOffsetCommitRetryCandidate::try_new(
            original,
            version,
            Err(RequestError::Rejected {
                failure: CallFailure::NotReady,
                delivery: Delivery::NotSent,
            }),
        )
        .unwrap_or_else(|_| panic!("known unsent route loss is retryable"));
        let replacement = candidate.into_prepared();
        assert_eq!(replacement.operation_deadline(), deadline);
        assert_eq!(
            replacement.operation_id(),
            kafka_client_core::OperationId::from_raw(41)
        );
        assert_eq!(
            crate::protocol::consumer::group_offset_commit_request(&replacement),
            request
        );
    }
}

#[test]
fn uncertain_delivery_expiry_and_unrelated_failures_cannot_be_replaced() {
    for (version, failure, delivery) in [
        (None, CallFailure::NotReady, Delivery::PossiblySent),
        (None, CallFailure::DeadlineExceeded, Delivery::NotSent),
        (None, CallFailure::Closed, Delivery::NotSent),
        (
            Some(ApiVersion::new(1)),
            CallFailure::NotReady,
            Delivery::NotSent,
        ),
    ] {
        assert!(
            GroupOffsetCommitRetryCandidate::try_new(
                prepared(42),
                version,
                Err(RequestError::Rejected { failure, delivery }),
            )
            .is_err()
        );
    }
    assert!(
        GroupOffsetCommitRetryCandidate::try_new(prepared(42), None, Ok(response(16))).is_err()
    );
}

#[test]
fn unsent_candidate_fallback_preserves_delivery_certainty() {
    let candidate = GroupOffsetCommitRetryCandidate::try_new(
        prepared(43),
        None,
        Err(RequestError::Rejected {
            failure: CallFailure::NotReady,
            delivery: Delivery::NotSent,
        }),
    )
    .unwrap_or_else(|_| panic!("known unsent candidate"));
    assert_eq!(
        candidate.into_terminal(),
        GroupOffsetCommitInput::TransportFailed {
            delivery: DeliveryStatus::NotSent,
        }
    );
}

#[test]
fn missing_causal_route_token_cannot_authorize_replacement() {
    let settled = super::classify_group_offset_commit_settlement(
        prepared(44),
        None,
        Err(RequestError::Rejected {
            failure: CallFailure::NotReady,
            delivery: Delivery::NotSent,
        }),
        None,
        false,
    );
    assert!(!settled.is_retry_ready());
    let (input, confirmation) = settled.into_parts();
    assert_eq!(
        input,
        GroupOffsetCommitInput::TransportFailed {
            delivery: DeliveryStatus::NotSent,
        }
    );
    confirmation.confirm_group_commit_route_token();
}

fn response(error_code: i16) -> OffsetCommitResponse {
    let mut partition = OffsetCommitResponsePartition::default();
    partition.partition_index = 0;
    partition.error_code = error_code;
    let mut topic = OffsetCommitResponseTopic::default();
    topic.name = "orders".into();
    topic.partitions.push(partition);
    let mut response = OffsetCommitResponse::default();
    response.topics.push(topic);
    response
}
