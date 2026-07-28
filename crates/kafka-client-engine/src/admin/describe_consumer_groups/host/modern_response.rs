//! KIP-848 protocol values translated into deterministic core descriptions.

use core::num::NonZeroI16;

use kafka_client_core::{
    AdminConsumerGroupAssignment, AdminConsumerGroupBrokerError, AdminConsumerGroupDescription,
    AdminConsumerGroupDescriptionDetails, AdminConsumerGroupDescriptionMember,
    AdminConsumerGroupDescriptionOutcome, AdminConsumerGroupMemberDetails,
    AdminConsumerGroupTopicPartitions, AdminModernConsumerGroupDetails,
    AdminModernConsumerGroupMemberDetails,
};

use crate::protocol::admin::describe_consumer_groups::{
    ConsumerGroupDescribeAssignment, ConsumerGroupDescribeDescription, ConsumerGroupDescribeMember,
    ConsumerGroupDescribeResult, ConsumerGroupDescribeTopicPartitions,
};

pub(super) fn modern_outcome(
    group_id: String,
    result: ConsumerGroupDescribeResult,
) -> Option<AdminConsumerGroupDescriptionOutcome> {
    match result {
        ConsumerGroupDescribeResult::Described(description) => {
            Some(AdminConsumerGroupDescriptionOutcome::described(
                group_id,
                modern_description(description),
            ))
        }
        ConsumerGroupDescribeResult::Failed(error) => {
            let (code, message, truncated) = error.into_parts();
            Some(AdminConsumerGroupDescriptionOutcome::broker_failed(
                group_id,
                AdminConsumerGroupBrokerError::new(NonZeroI16::new(code)?, message, truncated),
            ))
        }
    }
}

fn modern_description(
    description: ConsumerGroupDescribeDescription,
) -> AdminConsumerGroupDescription {
    let (state, group_epoch, assignment_epoch, assignor, members, operations) =
        description.into_parts();
    AdminConsumerGroupDescription::new(
        state,
        AdminConsumerGroupDescriptionDetails::Consumer(AdminModernConsumerGroupDetails::new(
            group_epoch,
            assignment_epoch,
            assignor,
        )),
        members.into_iter().map(modern_member).collect(),
        operations,
    )
}

fn modern_member(member: ConsumerGroupDescribeMember) -> AdminConsumerGroupDescriptionMember {
    let (
        member_id,
        instance_id,
        rack_id,
        member_epoch,
        client_id,
        client_host,
        subscriptions,
        subscription_regex,
        assignment,
        target_assignment,
        member_type,
    ) = member.into_parts();
    AdminConsumerGroupDescriptionMember::new(
        member_id,
        instance_id,
        client_id,
        client_host,
        AdminConsumerGroupMemberDetails::Consumer(AdminModernConsumerGroupMemberDetails::new(
            rack_id,
            member_epoch,
            subscriptions,
            subscription_regex,
            modern_assignment(assignment),
            modern_assignment(target_assignment),
            member_type,
        )),
    )
}

fn modern_assignment(assignment: ConsumerGroupDescribeAssignment) -> AdminConsumerGroupAssignment {
    AdminConsumerGroupAssignment::new(
        assignment
            .into_topics()
            .into_iter()
            .map(modern_topic_partitions)
            .collect(),
    )
}

fn modern_topic_partitions(
    topic: ConsumerGroupDescribeTopicPartitions,
) -> AdminConsumerGroupTopicPartitions {
    let (topic_id, topic_name, partitions) = topic.into_parts();
    AdminConsumerGroupTopicPartitions::new(topic_id, topic_name, partitions)
}
