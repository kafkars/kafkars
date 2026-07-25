//! Version and authoritative failure normalization for group `OffsetCommit`.

use std::{sync::Arc, time::Instant};

use kafka_client_core::{
    AssignmentGeneration, Deadline, DeliveryStatus, GroupCheckpoint, GroupCheckpointEntry, GroupId,
    GroupOffsetCommitEffect, GroupOffsetCommitInput, GroupOffsetCommitPartitionResult, MemberId,
    OperationId, PartitionIndex, TopicId,
};
use kafka_driver::{ApiKey, ApiVersion, CallFailure, Delivery, RequestError};
use kafka_wire::{
    OffsetCommitResponse,
    offset_commit_response::{OffsetCommitResponsePartition, OffsetCommitResponseTopic},
};
use kafka_wire_core::{DecodeError, EncodeError};

use crate::{
    clock::OperationDeadline,
    protocol::consumer::{
        ClassicGroupCommitSession, GroupOffsetCommitResultReservation, GroupOffsetCommitTopicName,
        PreparedGroupOffsetCommit,
    },
};

use super::group_offset_commit_terminal::normalize_group_offset_commit_terminal;

#[test]
fn every_selected_v2_through_v9_restores_broker_facts() {
    for version in 2..=9 {
        let GroupOffsetCommitInput::BrokerResponded {
            throttle_time_ms,
            outcomes,
        } = normalize_group_offset_commit_terminal(
            prepared(),
            Some(ApiVersion::new(version)),
            Ok(response(11, "orders", 0, -17)),
        )
        else {
            panic!("v{version} broker response expected");
        };
        assert_eq!(throttle_time_ms, 11);
        assert!(matches!(
            outcomes[0].result(),
            GroupOffsetCommitPartitionResult::Committed
        ));
        let GroupOffsetCommitPartitionResult::Rejected(error) = outcomes[1].result() else {
            panic!("exact rejection expected");
        };
        assert_eq!(error.code(), -17);
    }
}

#[test]
fn selected_version_is_required_and_bounded_to_v2_v9() {
    assert_eq!(
        normalize_group_offset_commit_terminal(prepared(), None, Ok(response(0, "orders", 0, 0)),),
        GroupOffsetCommitInput::InvalidResponse
    );
    for version in [1, 10] {
        assert_eq!(
            normalize_group_offset_commit_terminal(
                prepared(),
                Some(ApiVersion::new(version)),
                Ok(response(0, "orders", 0, 0)),
            ),
            GroupOffsetCommitInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent
            }
        );
    }
}

#[test]
fn malformed_oversized_and_negative_throttle_responses_are_invalid() {
    let mut oversized_topic = OffsetCommitResponseTopic::default();
    oversized_topic.name = "orders".into();
    oversized_topic.partitions = (0..65)
        .map(|partition| response_partition(partition, 0))
        .collect();
    let mut oversized = OffsetCommitResponse::default();
    oversized.topics.push(oversized_topic);
    assert_eq!(
        normalize_group_offset_commit_terminal(prepared(), Some(ApiVersion::new(9)), Ok(oversized),),
        GroupOffsetCommitInput::InvalidResponse
    );
    assert_eq!(
        normalize_group_offset_commit_terminal(
            prepared(),
            Some(ApiVersion::new(9)),
            Ok(response(-1, "orders", 0, 0)),
        ),
        GroupOffsetCommitInput::InvalidResponse
    );
}

#[test]
fn selected_v5_is_incompatible_when_checkpoint_requires_leader_epoch() {
    assert_eq!(
        normalize_group_offset_commit_terminal(
            prepared_with_epoch(true),
            Some(ApiVersion::new(5)),
            Ok(response(0, "orders", 0, 0)),
        ),
        GroupOffsetCommitInput::ProtocolIncompatible {
            delivery: DeliveryStatus::PossiblySent
        }
    );
    assert!(matches!(
        normalize_group_offset_commit_terminal(
            prepared_with_epoch(true),
            Some(ApiVersion::new(6)),
            Ok(response(0, "orders", 0, 0)),
        ),
        GroupOffsetCommitInput::BrokerResponded { .. }
    ));
}

#[test]
fn decode_deadline_and_transport_failures_preserve_authoritative_delivery() {
    assert_eq!(
        normalize_group_offset_commit_terminal(
            prepared(),
            Some(ApiVersion::new(9)),
            Err(RequestError::Decode(DecodeError::UnexpectedEnd {
                offset: 1,
                needed: 4,
                remaining: 0,
            })),
        ),
        GroupOffsetCommitInput::InvalidResponse
    );
    assert_eq!(
        normalize_group_offset_commit_terminal(
            prepared(),
            Some(ApiVersion::new(9)),
            Err(RequestError::Rejected {
                failure: CallFailure::DeadlineExceeded,
                delivery: Delivery::PossiblySent,
            }),
        ),
        GroupOffsetCommitInput::DeadlineElapsed {
            delivery: DeliveryStatus::PossiblySent
        }
    );
    assert_eq!(
        normalize_group_offset_commit_terminal(
            prepared(),
            None,
            Err(RequestError::RouteUnavailable),
        ),
        GroupOffsetCommitInput::TransportFailed {
            delivery: DeliveryStatus::NotSent
        }
    );
}

#[test]
fn compatibility_failures_are_definitely_unsent() {
    let failures = [
        RequestError::Encode(EncodeError::LengthOverflow {
            kind: "group id",
            length: usize::MAX,
            maximum: i16::MAX as usize,
        }),
        RequestError::UnsupportedVersion {
            message: "OffsetCommit request",
            version: ApiVersion::new(10),
        },
        RequestError::ApiUnavailable {
            api_key: ApiKey::new(8),
        },
        RequestError::VersionLimitUnavailable {
            api_key: ApiKey::new(8),
            maximum: ApiVersion::new(9),
            negotiated_minimum: ApiVersion::new(10),
        },
        RequestError::VersionFloorUnavailable {
            api_key: ApiKey::new(8),
            minimum: ApiVersion::new(6),
            negotiated_maximum: ApiVersion::new(5),
        },
        RequestError::VersionBoundsInvalid {
            api_key: ApiKey::new(8),
            minimum: ApiVersion::new(6),
            maximum: ApiVersion::new(5),
        },
    ];
    for failure in failures {
        assert_eq!(
            normalize_group_offset_commit_terminal(prepared(), None, Err(failure)),
            GroupOffsetCommitInput::ProtocolIncompatible {
                delivery: DeliveryStatus::NotSent
            }
        );
    }
}

fn prepared() -> PreparedGroupOffsetCommit {
    prepared_with_epoch(false)
}

fn prepared_with_epoch(requires_epoch: bool) -> PreparedGroupOffsetCommit {
    let result_reservation = GroupOffsetCommitResultReservation::try_new(2)
        .unwrap_or_else(|error| panic!("reserve result capacity: {error:?}"));
    let deadline = OperationDeadline::from_parts_for_test(Deadline::from_tick(100), Instant::now());
    let checkpoint = GroupCheckpoint::try_new(
        group_id(),
        member_id(),
        generation(),
        vec![
            checkpoint_entry(0, 10, requires_epoch.then_some(7)),
            checkpoint_entry(1, 20, None),
        ],
    )
    .unwrap_or_else(|error| panic!("valid checkpoint: {error}"));
    let effect = GroupOffsetCommitEffect::Submit {
        operation_id: OperationId::from_raw(9),
        deadline: deadline.core(),
        checkpoint,
    };
    let session = ClassicGroupCommitSession::new(
        group_id(),
        Arc::from("readers"),
        member_id(),
        Arc::from("member-a"),
        generation(),
        4,
    );
    PreparedGroupOffsetCommit::from_effect(
        effect,
        deadline,
        session,
        vec![GroupOffsetCommitTopicName::new(
            TopicId::from_raw(1),
            Arc::from("orders"),
        )],
        result_reservation,
    )
    .unwrap_or_else(|error| panic!("valid prepared commit: {:?}", error.kind()))
}

fn checkpoint_entry(
    partition: u32,
    next_offset: i64,
    leader_epoch: Option<i32>,
) -> GroupCheckpointEntry {
    GroupCheckpointEntry::try_new(
        TopicId::from_raw(1),
        PartitionIndex::from_raw(partition),
        next_offset,
        leader_epoch,
    )
    .unwrap_or_else(|error| panic!("valid entry: {error}"))
}

fn response(
    throttle_time_ms: i32,
    topic_name: &str,
    first_code: i16,
    second_code: i16,
) -> OffsetCommitResponse {
    let mut topic = OffsetCommitResponseTopic::default();
    topic.name = topic_name.into();
    topic.partitions = vec![
        response_partition(0, first_code),
        response_partition(1, second_code),
    ];
    let mut response = OffsetCommitResponse::default();
    response.throttle_time_ms = throttle_time_ms;
    response.topics.push(topic);
    response
}

fn response_partition(partition_index: i32, error_code: i16) -> OffsetCommitResponsePartition {
    let mut partition = OffsetCommitResponsePartition::default();
    partition.partition_index = partition_index;
    partition.error_code = error_code;
    partition
}

fn group_id() -> GroupId {
    GroupId::try_from_raw(1).unwrap_or_else(|| panic!("nonzero group"))
}

fn member_id() -> MemberId {
    MemberId::try_from_raw(2).unwrap_or_else(|| panic!("nonzero member"))
}

fn generation() -> AssignmentGeneration {
    AssignmentGeneration::try_from_raw(4).unwrap_or_else(|| panic!("nonzero generation"))
}
