//! Fallible deterministic materialization of a validated API-77 response.

use core::num::NonZeroI16;

use kafka_wire::share_group_describe_response::{Assignment, DescribedGroup, Member};

use super::{
    DescribeShareGroupAssignment, DescribeShareGroupBrokerError, DescribeShareGroupDescription,
    DescribeShareGroupMember, DescribeShareGroupProtocolFailure, DescribeShareGroupResult,
    DescribeShareGroupTopicPartitions,
    retention::{bounded_diagnostic, clone_string},
};

pub(super) fn materialize_group(
    group: &DescribedGroup,
    include_authorized_operations: bool,
) -> Result<DescribeShareGroupResult, DescribeShareGroupProtocolFailure> {
    if let Some(code) = NonZeroI16::new(group.error_code) {
        let (message, truncated) = bounded_diagnostic(group.error_message.as_deref())?;
        return Ok(DescribeShareGroupResult::Failed(
            DescribeShareGroupBrokerError::new(code, message, truncated),
        ));
    }
    Ok(DescribeShareGroupResult::Described(
        materialize_description(group, include_authorized_operations)?,
    ))
}

fn materialize_description(
    group: &DescribedGroup,
    include_authorized_operations: bool,
) -> Result<DescribeShareGroupDescription, DescribeShareGroupProtocolFailure> {
    let mut members = Vec::new();
    members
        .try_reserve_exact(group.members.len())
        .map_err(|_| DescribeShareGroupProtocolFailure::Allocation)?;
    for member in &group.members {
        members.push(materialize_member(member)?);
    }
    members.sort_unstable_by(|left, right| {
        left.member_id()
            .as_bytes()
            .cmp(right.member_id().as_bytes())
    });
    Ok(DescribeShareGroupDescription::new(
        clone_string(group.group_state.as_str())?,
        group.group_epoch,
        group.assignment_epoch,
        clone_string(group.assignor_name.as_str())?,
        members,
        include_authorized_operations
            .then_some(group.authorized_operations)
            .filter(|operations| *operations != i32::MIN),
    ))
}

fn materialize_member(
    member: &Member,
) -> Result<DescribeShareGroupMember, DescribeShareGroupProtocolFailure> {
    let mut subscriptions = Vec::new();
    subscriptions
        .try_reserve_exact(member.subscribed_topic_names.len())
        .map_err(|_| DescribeShareGroupProtocolFailure::Allocation)?;
    for topic in &member.subscribed_topic_names {
        subscriptions.push(clone_string(topic.as_str())?);
    }
    subscriptions.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    Ok(DescribeShareGroupMember::new(
        clone_string(member.member_id.as_str())?,
        member.rack_id.as_deref().map(clone_string).transpose()?,
        member.member_epoch,
        clone_string(member.client_id.as_str())?,
        clone_string(member.client_host.as_str())?,
        subscriptions,
        materialize_assignment(&member.assignment)?,
    ))
}

fn materialize_assignment(
    assignment: &Assignment,
) -> Result<DescribeShareGroupAssignment, DescribeShareGroupProtocolFailure> {
    let mut topics = Vec::new();
    topics
        .try_reserve_exact(assignment.topic_partitions.len())
        .map_err(|_| DescribeShareGroupProtocolFailure::Allocation)?;
    for topic in &assignment.topic_partitions {
        let mut partitions = Vec::new();
        partitions
            .try_reserve_exact(topic.partitions.len())
            .map_err(|_| DescribeShareGroupProtocolFailure::Allocation)?;
        partitions.extend(topic.partitions.iter().copied());
        partitions.sort_unstable();
        topics.push(DescribeShareGroupTopicPartitions::new(
            topic.topic_id.to_bytes(),
            clone_string(topic.topic_name.as_str())?,
            partitions,
        ));
    }
    topics.sort_unstable_by(|left, right| {
        left.topic_id().cmp(right.topic_id()).then_with(|| {
            left.topic_name()
                .as_bytes()
                .cmp(right.topic_name().as_bytes())
        })
    });
    Ok(DescribeShareGroupAssignment::new(topics))
}
