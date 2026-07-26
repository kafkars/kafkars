//! Bounded `OffsetCommit` response validation before charged correlation.

use kafka_wire::OffsetCommitResponse;

use super::{
    OffsetCommitTargetRef, ValidatedOffsetCommitResponse, correlation::correlate_response,
    retention::validated_result_charge, version::validate_selected_version,
};

/// Generated response facts unsafe to bind to an offset-alteration operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupOffsetAlterProtocolFailure {
    UnsupportedApiVersion {
        minimum: i16,
        maximum: i16,
        actual: i16,
    },
    NegativeThrottleTime {
        actual: i32,
    },
    TopicCount {
        expected: usize,
        actual: usize,
    },
    UnexpectedTopic,
    MissingTopic,
    DuplicateTopic,
    EmptyTopic,
    EmptyTopicPartitions,
    PartitionCount {
        expected: usize,
        actual: usize,
    },
    UnexpectedPartition {
        actual: i32,
    },
    MissingPartition {
        actual: i32,
    },
    DuplicatePartition {
        actual: i32,
    },
    DuplicateTarget {
        actual: i32,
    },
    NegativePartition {
        actual: i32,
    },
    RetainedBytes,
}

/// Validates before allocation and restores exact caller target order.
pub(crate) fn validate_group_offset_alter_response<'a>(
    targets: &[OffsetCommitTargetRef<'_>],
    response: &'a OffsetCommitResponse,
    selected_version: i16,
    result_limit: usize,
) -> Result<ValidatedOffsetCommitResponse<'a>, GroupOffsetAlterProtocolFailure> {
    validate_selected_version(targets, selected_version).map_err(|failure| {
        GroupOffsetAlterProtocolFailure::UnsupportedApiVersion {
            minimum: failure.minimum,
            maximum: failure.maximum,
            actual: failure.actual,
        }
    })?;
    let throttle_time_ms = validate_throttle(response, selected_version)?;
    let (entry_count, retained_charge) =
        validated_result_charge(targets.iter().map(|target| target.topic()))
            .ok_or(GroupOffsetAlterProtocolFailure::RetainedBytes)?;
    ensure_limit(retained_charge, result_limit)?;
    let entries = correlate_response(targets, &response.topics, entry_count)?;
    Ok(ValidatedOffsetCommitResponse::new(
        entries,
        throttle_time_ms,
        retained_charge,
    ))
}

fn validate_throttle(
    response: &OffsetCommitResponse,
    selected_version: i16,
) -> Result<u32, GroupOffsetAlterProtocolFailure> {
    if selected_version < 3 {
        return Ok(0);
    }
    u32::try_from(response.throttle_time_ms).map_err(|_| {
        GroupOffsetAlterProtocolFailure::NegativeThrottleTime {
            actual: response.throttle_time_ms,
        }
    })
}

fn ensure_limit(charge: usize, limit: usize) -> Result<(), GroupOffsetAlterProtocolFailure> {
    (charge <= limit)
        .then_some(())
        .ok_or(GroupOffsetAlterProtocolFailure::RetainedBytes)
}
