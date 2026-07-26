//! Bounded v0 response validation before charged sort/merge correlation.

use core::num::NonZeroI16;

use kafka_wire::OffsetDeleteResponse;

use super::{
    OffsetDeleteTargetRef, ValidatedOffsetDeleteResponse, correlation::correlate_response,
    retention::validated_result_charge,
};

/// Generated response facts unsafe to bind to an offset-deletion operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupOffsetDeleteProtocolFailure {
    UnsupportedApiVersion { actual: i16 },
    NegativeThrottleTime { actual: i32 },
    TopicCount { expected: usize, actual: usize },
    UnexpectedTopic,
    MissingTopic,
    DuplicateTopic,
    EmptyTopic,
    EmptyTopicPartitions,
    PartitionCount { expected: usize, actual: usize },
    UnexpectedPartition { actual: i32 },
    MissingPartition { actual: i32 },
    DuplicatePartition { actual: i32 },
    DuplicateTarget { actual: i32 },
    NegativePartition { actual: i32 },
    RetainedBytes,
}

/// Validates before allocation and restores exact caller target order.
pub(crate) fn validate_group_offset_delete_response<'a>(
    targets: &[OffsetDeleteTargetRef<'_>],
    response: &'a OffsetDeleteResponse,
    selected_version: i16,
    result_limit: usize,
) -> Result<ValidatedOffsetDeleteResponse<'a>, GroupOffsetDeleteProtocolFailure> {
    validate_version(selected_version)?;
    let throttle_time_ms = validate_throttle(response.throttle_time_ms)?;
    if let Some(code) = NonZeroI16::new(response.error_code) {
        return top_level_rejection(throttle_time_ms, code, result_limit);
    }
    let (entry_count, retained_charge) =
        validated_result_charge(targets.iter().map(|target| target.topic()))
            .ok_or(GroupOffsetDeleteProtocolFailure::RetainedBytes)?;
    ensure_limit(retained_charge, result_limit)?;
    let entries = correlate_response(targets, &response.topics, entry_count)?;
    Ok(ValidatedOffsetDeleteResponse::new(
        entries,
        throttle_time_ms,
        None,
        retained_charge,
    ))
}

fn validate_version(selected_version: i16) -> Result<(), GroupOffsetDeleteProtocolFailure> {
    (selected_version == 0).then_some(()).ok_or(
        GroupOffsetDeleteProtocolFailure::UnsupportedApiVersion {
            actual: selected_version,
        },
    )
}

fn validate_throttle(actual: i32) -> Result<u32, GroupOffsetDeleteProtocolFailure> {
    u32::try_from(actual)
        .map_err(|_| GroupOffsetDeleteProtocolFailure::NegativeThrottleTime { actual })
}

fn top_level_rejection(
    throttle_time_ms: u32,
    code: NonZeroI16,
    result_limit: usize,
) -> Result<ValidatedOffsetDeleteResponse<'static>, GroupOffsetDeleteProtocolFailure> {
    let (_, retained_charge) = validated_result_charge(core::iter::empty())
        .ok_or(GroupOffsetDeleteProtocolFailure::RetainedBytes)?;
    ensure_limit(retained_charge, result_limit)?;
    Ok(ValidatedOffsetDeleteResponse::new(
        Vec::new(),
        throttle_time_ms,
        Some(code),
        retained_charge,
    ))
}

fn ensure_limit(charge: usize, limit: usize) -> Result<(), GroupOffsetDeleteProtocolFailure> {
    (charge <= limit)
        .then_some(())
        .ok_or(GroupOffsetDeleteProtocolFailure::RetainedBytes)
}
