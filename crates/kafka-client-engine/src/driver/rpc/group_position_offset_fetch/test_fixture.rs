//! Semantic group-position terminal fixtures that keep wire and driver types in the adapter.

use kafka_driver::{ApiKey, ApiVersion, CallFailure, CompletionError, Delivery, RequestError};
use kafka_wire::{
    OffsetFetchResponse,
    offset_fetch_response::{OffsetFetchResponsePartition, OffsetFetchResponseTopic},
};
use kafka_wire_core::DecodeError;

use super::{
    calls::TrackedGroupPositionOffsetFetchCalls, key::GroupPositionOffsetFetchKey,
    recovery::GroupPositionOffsetFetchCompletionFailureKind,
    terminal::GroupPositionOffsetFetchDriverFailureKind,
};

#[derive(Clone, Copy)]
pub(crate) enum GroupPositionOffsetFetchTestPartition {
    Committed(i64),
    CommittedWithMetadataBytes { offset: i64, bytes: usize },
    Missing,
    Rejected(i16),
}

impl TrackedGroupPositionOffsetFetchCalls {
    pub(crate) fn install_legacy_terminal_for_test(
        &mut self,
        key: GroupPositionOffsetFetchKey,
        selected_version: Option<i16>,
        throttle_time_ms: i32,
        group_error: i16,
        values: &[(i32, GroupPositionOffsetFetchTestPartition)],
    ) {
        self.install_terminal_for_test(
            key,
            selected_version,
            Ok(legacy_response(throttle_time_ms, group_error, values)),
        );
    }

    pub(crate) fn install_empty_terminal_for_test(
        &mut self,
        key: GroupPositionOffsetFetchKey,
        selected_version: Option<i16>,
    ) {
        self.install_terminal_for_test(key, selected_version, Ok(OffsetFetchResponse::default()));
    }

    pub(crate) fn install_driver_failure_kind_for_test(
        &mut self,
        key: GroupPositionOffsetFetchKey,
        kind: GroupPositionOffsetFetchDriverFailureKind,
    ) {
        let error = match kind {
            GroupPositionOffsetFetchDriverFailureKind::DeadlineElapsed => RequestError::Rejected {
                failure: CallFailure::DeadlineExceeded,
                delivery: Delivery::PossiblySent,
            },
            GroupPositionOffsetFetchDriverFailureKind::Compatibility => {
                RequestError::VersionFloorUnavailable {
                    api_key: ApiKey::new(9),
                    minimum: ApiVersion::new(7),
                    negotiated_maximum: ApiVersion::new(6),
                }
            }
            GroupPositionOffsetFetchDriverFailureKind::InvalidResponse => {
                RequestError::Decode(DecodeError::UnexpectedEnd {
                    offset: 1,
                    needed: 4,
                    remaining: 0,
                })
            }
            GroupPositionOffsetFetchDriverFailureKind::Transport => RequestError::RouteUnavailable,
        };
        self.install_terminal_for_test(key, None, Err(error));
    }

    pub(crate) fn install_completion_failure_kind_for_test(
        &mut self,
        key: GroupPositionOffsetFetchKey,
        kind: GroupPositionOffsetFetchCompletionFailureKind,
    ) {
        let source = match kind {
            GroupPositionOffsetFetchCompletionFailureKind::Closed => CompletionError::Closed,
            GroupPositionOffsetFetchCompletionFailureKind::Consumed => CompletionError::Consumed,
            GroupPositionOffsetFetchCompletionFailureKind::Unknown => {
                panic!("no synthetic unknown completion failure")
            }
        };
        self.install_completion_failure_for_test(key, source);
    }
}

fn legacy_response(
    throttle_time_ms: i32,
    group_error: i16,
    values: &[(i32, GroupPositionOffsetFetchTestPartition)],
) -> OffsetFetchResponse {
    let mut response = OffsetFetchResponse::default();
    response.throttle_time_ms = throttle_time_ms;
    response.error_code = group_error;
    if group_error == 0 {
        let mut topic = OffsetFetchResponseTopic::default();
        topic.name = "orders".into();
        topic.partitions = values
            .iter()
            .map(|(index, value)| legacy_partition(*index, *value))
            .collect();
        response.topics = vec![topic];
    }
    response
}

fn legacy_partition(
    index: i32,
    value: GroupPositionOffsetFetchTestPartition,
) -> OffsetFetchResponsePartition {
    let mut partition = OffsetFetchResponsePartition::default();
    partition.partition_index = index;
    partition.committed_leader_epoch = -1;
    match value {
        GroupPositionOffsetFetchTestPartition::Committed(offset) => {
            partition.committed_offset = offset;
        }
        GroupPositionOffsetFetchTestPartition::CommittedWithMetadataBytes { offset, bytes } => {
            partition.committed_offset = offset;
            partition.metadata = Some("x".repeat(bytes).into());
        }
        GroupPositionOffsetFetchTestPartition::Missing => {
            partition.committed_offset = -1;
        }
        GroupPositionOffsetFetchTestPartition::Rejected(code) => {
            partition.committed_offset = -1;
            partition.error_code = code;
        }
    }
    partition
}
