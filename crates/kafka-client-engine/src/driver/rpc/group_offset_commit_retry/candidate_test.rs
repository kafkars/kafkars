//! Exact bounded eligibility and terminal fallback for coordinator commit replacement.

use kafka_client_core::{DeliveryStatus, GroupOffsetCommitInput, GroupOffsetCommitPartitionResult};
use kafka_driver::ApiVersion;
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
            response(code),
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
            response(code),
        );
        let Err((prepared, response)) = candidate else {
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
        response(16),
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
