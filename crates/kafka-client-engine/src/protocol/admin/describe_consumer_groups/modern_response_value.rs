//! Bounded wire-to-scalar copying after complete API-key 69 validation.

use core::num::NonZeroI16;

use kafka_wire::consumer_group_describe_response::{Assignment, DescribedGroup, Member};

use super::{
    modern_assignment::{ConsumerGroupDescribeAssignment, ConsumerGroupDescribeTopicPartitions},
    modern_model::{ConsumerGroupDescribeDescription, ConsumerGroupDescribeMember},
    modern_outcome::{
        ConsumerGroupDescribeBrokerError, ConsumerGroupDescribeFallback,
        ConsumerGroupDescribeResult,
    },
};

const MAX_DIAGNOSTIC_BYTES: usize = 1024;
const UNSUPPORTED_VERSION: i16 = 35;
const GROUP_ID_NOT_FOUND: i16 = 69;

pub(super) fn copy_group_result(
    group: &DescribedGroup,
    selected_version: i16,
    include_authorized_operations: bool,
) -> (
    ConsumerGroupDescribeResult,
    Option<ConsumerGroupDescribeFallback>,
) {
    if let Some(code) = NonZeroI16::new(group.error_code) {
        let (message, truncated) = bounded_diagnostic(group.error_message.as_deref());
        return (
            ConsumerGroupDescribeResult::Failed(ConsumerGroupDescribeBrokerError::new(
                code, message, truncated,
            )),
            broker_fallback(code),
        );
    }
    (
        ConsumerGroupDescribeResult::Described(copy_description(
            group,
            selected_version,
            include_authorized_operations,
        )),
        None,
    )
}

fn copy_description(
    group: &DescribedGroup,
    selected_version: i16,
    include_authorized_operations: bool,
) -> ConsumerGroupDescribeDescription {
    let mut members = group
        .members
        .iter()
        .map(|member| copy_member(member, selected_version))
        .collect::<Vec<_>>();
    members.sort_unstable_by(|left, right| {
        left.member_id()
            .as_bytes()
            .cmp(right.member_id().as_bytes())
    });
    let authorized_operations = include_authorized_operations
        .then_some(group.authorized_operations)
        .filter(|operations| *operations != i32::MIN);
    ConsumerGroupDescribeDescription::new(
        canonical_string(group.group_state.as_str()),
        group.group_epoch,
        group.assignment_epoch,
        canonical_string(group.assignor_name.as_str()),
        members,
        authorized_operations,
    )
}

fn copy_member(member: &Member, selected_version: i16) -> ConsumerGroupDescribeMember {
    let mut subscriptions = member
        .subscribed_topic_names
        .iter()
        .map(|topic| canonical_string(topic.as_str()))
        .collect::<Vec<_>>();
    subscriptions.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    ConsumerGroupDescribeMember::new(
        canonical_string(member.member_id.as_str()),
        member.instance_id.as_deref().map(canonical_string),
        member.rack_id.as_deref().map(canonical_string),
        member.member_epoch,
        canonical_string(member.client_id.as_str()),
        canonical_string(member.client_host.as_str()),
        subscriptions,
        member
            .subscribed_topic_regex
            .as_deref()
            .map(canonical_string),
        copy_assignment(&member.assignment),
        copy_assignment(&member.target_assignment),
        (selected_version >= 1).then_some(member.member_type),
    )
}

fn copy_assignment(assignment: &Assignment) -> ConsumerGroupDescribeAssignment {
    let mut topics = assignment
        .topic_partitions
        .iter()
        .map(|topic| {
            let mut partitions = topic.partitions.clone();
            partitions.sort_unstable();
            ConsumerGroupDescribeTopicPartitions::new(
                topic.topic_id.to_bytes(),
                canonical_string(topic.topic_name.as_str()),
                partitions,
            )
        })
        .collect::<Vec<_>>();
    topics.sort_unstable_by(|left, right| {
        left.topic_id().cmp(right.topic_id()).then_with(|| {
            left.topic_name()
                .as_bytes()
                .cmp(right.topic_name().as_bytes())
        })
    });
    ConsumerGroupDescribeAssignment::new(topics)
}

fn broker_fallback(code: NonZeroI16) -> Option<ConsumerGroupDescribeFallback> {
    match code.get() {
        UNSUPPORTED_VERSION => Some(ConsumerGroupDescribeFallback::BrokerUnsupportedVersion),
        GROUP_ID_NOT_FOUND => Some(ConsumerGroupDescribeFallback::BrokerGroupIdNotFound),
        _ => None,
    }
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
