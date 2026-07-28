//! Strict bounded normalization of one singleton `ConsumerGroupDescribe` response.

use kafka_wire::{ConsumerGroupDescribeResponse, consumer_group_describe_response::DescribedGroup};

use super::{
    modern_outcome::NormalizedConsumerGroupDescribeResponse,
    modern_response_validation::validate_group, modern_response_value::copy_group_result,
};

/// Structural, compatibility, or retained-capacity rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ConsumerGroupDescribeResponseFailure {
    LocalUnsupportedVersion { actual: i16 },
    NegativeThrottleTime { actual: i32 },
    MissingGroup,
    DuplicateGroup,
    UnexpectedGroup,
    EmptyMemberId,
    DuplicateMemberId,
    EmptyInstanceId,
    EmptySubscription,
    DuplicateSubscription,
    TopicId,
    EmptyTopicName,
    DuplicateTopicId,
    DuplicateTopicName,
    Partition,
    DuplicatePartition,
    ScalarTooLarge,
    ResponseTooLarge,
}

/// Correlates and copies one generated response only after a complete bounded pass.
pub(crate) fn normalize_consumer_group_describe_response(
    expected_group_id: &str,
    include_authorized_operations: bool,
    selected_version: i16,
    response: &ConsumerGroupDescribeResponse,
    retained_limit: usize,
) -> Result<NormalizedConsumerGroupDescribeResponse, ConsumerGroupDescribeResponseFailure> {
    if !(0..=1).contains(&selected_version) {
        return Err(
            ConsumerGroupDescribeResponseFailure::LocalUnsupportedVersion {
                actual: selected_version,
            },
        );
    }
    let throttle_time_ms = u32::try_from(response.throttle_time_ms).map_err(|_| {
        ConsumerGroupDescribeResponseFailure::NegativeThrottleTime {
            actual: response.throttle_time_ms,
        }
    })?;
    let group = matching_group(expected_group_id, &response.groups)?;
    let retained_bytes = validate_group(group, retained_limit)?;
    let (result, fallback) =
        copy_group_result(group, selected_version, include_authorized_operations);
    Ok(NormalizedConsumerGroupDescribeResponse::new(
        throttle_time_ms,
        canonical_string(expected_group_id),
        result,
        fallback,
        retained_bytes,
    ))
}

fn matching_group<'a>(
    expected: &str,
    groups: &'a [DescribedGroup],
) -> Result<&'a DescribedGroup, ConsumerGroupDescribeResponseFailure> {
    let mut matching = None;
    for group in groups {
        if group.group_id.as_str() != expected {
            return Err(ConsumerGroupDescribeResponseFailure::UnexpectedGroup);
        }
        if matching.replace(group).is_some() {
            return Err(ConsumerGroupDescribeResponseFailure::DuplicateGroup);
        }
    }
    matching.ok_or(ConsumerGroupDescribeResponseFailure::MissingGroup)
}

fn canonical_string(value: &str) -> String {
    value.to_owned().into_boxed_str().into_string()
}
