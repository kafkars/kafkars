//! Strict borrowed normalization of one singleton `DescribeGroups` response.

use core::{mem::size_of, num::NonZeroI16};
use std::collections::BTreeSet;

use kafka_client_core::{
    AdminClassicConsumerGroupDetails, AdminClassicConsumerGroupMemberDetails,
    AdminConsumerGroupBrokerError, AdminConsumerGroupDescription,
    AdminConsumerGroupDescriptionDetails, AdminConsumerGroupDescriptionMember,
    AdminConsumerGroupDescriptionOutcome, AdminConsumerGroupMemberDetails,
};
use kafka_wire::{
    DescribeGroupsResponse,
    describe_groups_response::{DescribedGroup, DescribedGroupMember},
};

const MAX_DIAGNOSTIC_BYTES: usize = 1024;

/// One correlated response fact safe for deterministic core application.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedDescribeConsumerGroupResponse {
    throttle_time_ms: u32,
    outcome: AdminConsumerGroupDescriptionOutcome,
    retained_bytes: usize,
}

impl NormalizedDescribeConsumerGroupResponse {
    /// Consumes normalized response facts.
    pub(crate) fn into_parts(self) -> (u32, AdminConsumerGroupDescriptionOutcome, usize) {
        (self.throttle_time_ms, self.outcome, self.retained_bytes)
    }
}

/// Structural, compatibility, or retained-capacity normalization failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeConsumerGroupResponseFailure {
    UnsupportedApiVersion { actual: i16 },
    AuthorizedOperationsUnavailable { actual: i16 },
    NegativeThrottleTime { actual: i32 },
    MissingGroup,
    DuplicateGroup,
    UnexpectedGroup,
    DuplicateMemberId,
    RetainedBytes,
}

/// Correlates one generated singleton response before any owned result allocation.
pub(crate) fn normalize_describe_consumer_group_response(
    expected_group_id: &str,
    include_authorized_operations: bool,
    selected_version: i16,
    response: &DescribeGroupsResponse,
    retained_limit: usize,
) -> Result<NormalizedDescribeConsumerGroupResponse, DescribeConsumerGroupResponseFailure> {
    if !(0..=6).contains(&selected_version) {
        return Err(
            DescribeConsumerGroupResponseFailure::UnsupportedApiVersion {
                actual: selected_version,
            },
        );
    }
    if include_authorized_operations && selected_version < 3 {
        return Err(
            DescribeConsumerGroupResponseFailure::AuthorizedOperationsUnavailable {
                actual: selected_version,
            },
        );
    }
    let throttle_time_ms = if selected_version == 0 {
        0
    } else {
        u32::try_from(response.throttle_time_ms).map_err(|_| {
            DescribeConsumerGroupResponseFailure::NegativeThrottleTime {
                actual: response.throttle_time_ms,
            }
        })?
    };
    let group = matching_group(expected_group_id, &response.groups)?;
    let retained_bytes = validate_retained(group, retained_limit)?;
    let outcome = if let Some(code) = NonZeroI16::new(group.error_code) {
        let (message, truncated) = bounded_diagnostic(group.error_message.as_deref());
        AdminConsumerGroupDescriptionOutcome::broker_failed(
            expected_group_id.to_owned(),
            AdminConsumerGroupBrokerError::new(code, message, truncated),
        )
    } else {
        let mut members = group.members.iter().map(copy_member).collect::<Vec<_>>();
        members.sort_unstable_by(|left, right| {
            left.member_id()
                .as_bytes()
                .cmp(right.member_id().as_bytes())
        });
        let authorized_operations = (include_authorized_operations && selected_version >= 3)
            .then_some(group.authorized_operations)
            .filter(|operations| *operations != i32::MIN);
        AdminConsumerGroupDescriptionOutcome::described(
            expected_group_id.to_owned(),
            AdminConsumerGroupDescription::new(
                canonical_string(group.group_state.as_str()),
                AdminConsumerGroupDescriptionDetails::Classic(
                    AdminClassicConsumerGroupDetails::new(
                        canonical_string(group.protocol_type.as_str()),
                        canonical_string(group.protocol_data.as_str()),
                    ),
                ),
                members,
                authorized_operations,
            ),
        )
    };
    Ok(NormalizedDescribeConsumerGroupResponse {
        throttle_time_ms,
        outcome,
        retained_bytes,
    })
}

fn matching_group<'a>(
    expected: &str,
    groups: &'a [DescribedGroup],
) -> Result<&'a DescribedGroup, DescribeConsumerGroupResponseFailure> {
    let mut matching = None;
    for group in groups {
        if group.group_id.as_str() != expected {
            return Err(DescribeConsumerGroupResponseFailure::UnexpectedGroup);
        }
        if matching.replace(group).is_some() {
            return Err(DescribeConsumerGroupResponseFailure::DuplicateGroup);
        }
    }
    matching.ok_or(DescribeConsumerGroupResponseFailure::MissingGroup)
}

fn validate_retained(
    group: &DescribedGroup,
    retained_limit: usize,
) -> Result<usize, DescribeConsumerGroupResponseFailure> {
    let diagnostic_bytes = group
        .error_message
        .as_deref()
        .map_or(0, |message| message.len().min(MAX_DIAGNOSTIC_BYTES));
    let mut bytes = size_of::<AdminConsumerGroupDescriptionOutcome>()
        .checked_add(group.group_id.len())
        .and_then(|value| value.checked_add(diagnostic_bytes))
        .ok_or(DescribeConsumerGroupResponseFailure::RetainedBytes)?;
    if group.error_code != 0 {
        return (bytes <= retained_limit)
            .then_some(bytes)
            .ok_or(DescribeConsumerGroupResponseFailure::RetainedBytes);
    }
    bytes = bytes
        .checked_add(size_of::<AdminConsumerGroupDescription>())
        .and_then(|value| value.checked_add(group.group_state.len()))
        .and_then(|value| value.checked_add(group.protocol_type.len()))
        .and_then(|value| value.checked_add(group.protocol_data.len()))
        .and_then(|value| {
            group
                .members
                .len()
                .checked_mul(size_of::<AdminConsumerGroupDescriptionMember>())
                .and_then(|member_owners| value.checked_add(member_owners))
        })
        .and_then(|value| {
            group
                .members
                .len()
                .checked_mul(size_of::<&DescribedGroupMember>())
                .and_then(|sort_scratch| value.checked_add(sort_scratch))
        })
        .ok_or(DescribeConsumerGroupResponseFailure::RetainedBytes)?;
    let mut identities = BTreeSet::new();
    for member in &group.members {
        if !identities.insert(member.member_id.as_str()) {
            return Err(DescribeConsumerGroupResponseFailure::DuplicateMemberId);
        }
        bytes = bytes
            .checked_add(member.member_id.len())
            .and_then(|value| {
                value.checked_add(member.group_instance_id.as_deref().map_or(0, str::len))
            })
            .and_then(|value| value.checked_add(member.client_id.len()))
            .and_then(|value| value.checked_add(member.client_host.len()))
            .and_then(|value| value.checked_add(member.member_metadata.len()))
            .and_then(|value| value.checked_add(member.member_assignment.len()))
            .ok_or(DescribeConsumerGroupResponseFailure::RetainedBytes)?;
    }
    (bytes <= retained_limit)
        .then_some(bytes)
        .ok_or(DescribeConsumerGroupResponseFailure::RetainedBytes)
}

fn copy_member(member: &DescribedGroupMember) -> AdminConsumerGroupDescriptionMember {
    AdminConsumerGroupDescriptionMember::new(
        canonical_string(member.member_id.as_str()),
        member.group_instance_id.as_deref().map(canonical_string),
        canonical_string(member.client_id.as_str()),
        canonical_string(member.client_host.as_str()),
        AdminConsumerGroupMemberDetails::Classic(AdminClassicConsumerGroupMemberDetails::new(
            member.member_metadata.to_vec(),
            member.member_assignment.to_vec(),
        )),
    )
}

fn bounded_diagnostic(message: Option<&str>) -> (Option<String>, bool) {
    let Some(message) = message else {
        return (None, false);
    };
    if message.len() <= MAX_DIAGNOSTIC_BYTES {
        return (Some(canonical_string(message)), false);
    }
    let mut end = MAX_DIAGNOSTIC_BYTES;
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    (Some(canonical_string(&message[..end])), true)
}

fn canonical_string(value: &str) -> String {
    value.to_owned().into_boxed_str().into_string()
}
