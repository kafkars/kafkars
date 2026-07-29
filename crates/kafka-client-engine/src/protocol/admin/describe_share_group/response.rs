//! Strict singleton API-77 v1 validation, correlation, and normalization.

use kafka_wire::{ShareGroupDescribeResponse, share_group_describe_response::DescribedGroup};

use super::{
    NormalizedDescribeShareGroupResponse,
    materialize::materialize_group,
    retention::{error_required_bytes, scratch_required_bytes, success_required_bytes},
    validation::validate_success_group,
};

/// Generated response facts unsafe to bind to the accepted API-77 operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeShareGroupProtocolFailure {
    MissingSelectedVersion,
    UnsupportedApiVersion { actual: i16 },
    NegativeThrottleTime { actual: i32 },
    MissingGroup,
    DuplicateGroup,
    UnexpectedGroup,
    DiagnosticOnSuccess,
    MembersOnGroupError,
    EmptyGroupState,
    NegativeGroupEpoch,
    UnexpectedAuthorizedOperations,
    TooManyMembers,
    EmptyMemberId,
    DuplicateMemberId,
    EmptyRackId,
    NegativeMemberEpoch,
    TooManySubscriptions,
    EmptySubscription,
    DuplicateSubscription,
    TooManyAssignmentTopics,
    ZeroTopicId,
    EmptyTopicName,
    DuplicateTopicId,
    DuplicateTopicName,
    TooManyPartitions,
    NegativePartition,
    DuplicatePartition,
    ScalarTooLarge,
    ResponseTextBytesExceeded,
    GroupDiagnosticTooLarge,
    RetainedBytesOverflow,
    RetainedBytes { required: usize, limit: usize },
    Allocation,
}

/// Validates and copies exactly one coordinator-correlated share group.
pub(crate) fn normalize_describe_share_group_response(
    expected_group_id: &str,
    include_authorized_operations: bool,
    selected_version: Option<i16>,
    response: &ShareGroupDescribeResponse,
    retained_limit: usize,
) -> Result<NormalizedDescribeShareGroupResponse, DescribeShareGroupProtocolFailure> {
    validate_version(selected_version)?;
    let throttle_time_ms = u32::try_from(response.throttle_time_ms).map_err(|_| {
        DescribeShareGroupProtocolFailure::NegativeThrottleTime {
            actual: response.throttle_time_ms,
        }
    })?;
    let group = matching_group(expected_group_id, &response.groups)?;
    let retained_bytes = if group.error_code == 0 {
        validate_success_group(group, include_authorized_operations)?;
        success_required_bytes(group)?
            .checked_add(scratch_required_bytes(group)?)
            .ok_or(DescribeShareGroupProtocolFailure::RetainedBytesOverflow)?
    } else {
        if !group.members.is_empty() {
            return Err(DescribeShareGroupProtocolFailure::MembersOnGroupError);
        }
        if group
            .error_message
            .as_ref()
            .is_some_and(|message| message.len() > super::retention::MAX_RESPONSE_TEXT_BYTES)
        {
            return Err(DescribeShareGroupProtocolFailure::GroupDiagnosticTooLarge);
        }
        error_required_bytes(group)?
    };
    if retained_bytes > retained_limit {
        return Err(DescribeShareGroupProtocolFailure::RetainedBytes {
            required: retained_bytes,
            limit: retained_limit,
        });
    }
    let result = materialize_group(group, include_authorized_operations)?;
    Ok(NormalizedDescribeShareGroupResponse::new(
        throttle_time_ms,
        clone_group_id(expected_group_id)?,
        result,
        retained_bytes,
    ))
}

fn validate_version(
    selected_version: Option<i16>,
) -> Result<(), DescribeShareGroupProtocolFailure> {
    match selected_version {
        None => Err(DescribeShareGroupProtocolFailure::MissingSelectedVersion),
        Some(1) => Ok(()),
        Some(actual) => Err(DescribeShareGroupProtocolFailure::UnsupportedApiVersion { actual }),
    }
}

fn matching_group<'a>(
    expected: &str,
    groups: &'a [DescribedGroup],
) -> Result<&'a DescribedGroup, DescribeShareGroupProtocolFailure> {
    let mut matching = None;
    for group in groups {
        if group.group_id.as_str() != expected {
            return Err(DescribeShareGroupProtocolFailure::UnexpectedGroup);
        }
        if matching.replace(group).is_some() {
            return Err(DescribeShareGroupProtocolFailure::DuplicateGroup);
        }
    }
    matching.ok_or(DescribeShareGroupProtocolFailure::MissingGroup)
}

fn clone_group_id(value: &str) -> Result<String, DescribeShareGroupProtocolFailure> {
    super::retention::clone_string(value)
}
