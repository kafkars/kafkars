//! Core-to-engine translation for explicit group descriptions and partial failures.

use kafka_client_core::{
    AdminConsumerGroupDescriptionDetails as CoreDescriptionDetails,
    AdminConsumerGroupDescriptionResult as CoreResult,
    AdminConsumerGroupMemberDetails as CoreMemberDetails,
    AdminDescribeConsumerGroupsFailureKind as CoreFailureKind,
    AdminDescribeConsumerGroupsTerminal as CoreTerminal, DeliveryStatus as CoreDeliveryStatus,
};

use super::super::{
    ClassicConsumerGroupDetails, ClassicConsumerGroupMemberDetails, ConsumerGroupAssignment,
    ConsumerGroupDescription, ConsumerGroupDescriptionDetails, ConsumerGroupDescriptionError,
    ConsumerGroupDescriptionMember, ConsumerGroupDescriptionResult, ConsumerGroupMemberDetails,
    ConsumerGroupTopicPartitions, DescribeConsumerGroupsBatch,
    DescribeConsumerGroupsDeliveryStatus, DescribeConsumerGroupsFailure,
    DescribeConsumerGroupsFailureKind, DescribeConsumerGroupsOutcome, ModernConsumerGroupDetails,
    ModernConsumerGroupMemberDetails,
};

pub(crate) fn translate_terminal(terminal: CoreTerminal) -> DescribeConsumerGroupsOutcome {
    match terminal {
        CoreTerminal::Described(batch) => {
            let (throttle_time_ms, outcomes) = batch.into_parts();
            DescribeConsumerGroupsOutcome::Groups(DescribeConsumerGroupsBatch {
                throttle_time_ms,
                groups: outcomes
                    .into_iter()
                    .map(|outcome| {
                        let (group_id, result) = outcome.into_parts();
                        let result = match result {
                            CoreResult::Described(description) => {
                                Ok(description_from_core(description))
                            }
                            CoreResult::BrokerFailed(error) => {
                                let (code, message, message_truncated) = error.into_parts();
                                Err(ConsumerGroupDescriptionError::Broker(
                                    super::super::ConsumerGroupBrokerError {
                                        code,
                                        message,
                                        message_truncated,
                                    },
                                ))
                            }
                            CoreResult::OperationFailed(failure) => {
                                Err(ConsumerGroupDescriptionError::Operation(failure_from_core(
                                    failure,
                                )))
                            }
                        };
                        ConsumerGroupDescriptionResult { group_id, result }
                    })
                    .collect(),
            })
        }
        CoreTerminal::Failed(failure) => {
            DescribeConsumerGroupsOutcome::Failed(failure_from_core(failure))
        }
    }
}

const fn failure_kind(kind: CoreFailureKind) -> DescribeConsumerGroupsFailureKind {
    match kind {
        CoreFailureKind::DeadlineElapsed => DescribeConsumerGroupsFailureKind::DeadlineElapsed,
        CoreFailureKind::DriverRejected => DescribeConsumerGroupsFailureKind::DriverRejected,
        CoreFailureKind::Transport => DescribeConsumerGroupsFailureKind::Transport,
        CoreFailureKind::ResponseTooLarge => DescribeConsumerGroupsFailureKind::ResponseTooLarge,
        CoreFailureKind::Compatibility => DescribeConsumerGroupsFailureKind::Compatibility,
        CoreFailureKind::InvalidResponse => DescribeConsumerGroupsFailureKind::InvalidResponse,
        CoreFailureKind::NotAttempted => DescribeConsumerGroupsFailureKind::NotAttempted,
    }
}

fn failure_from_core(
    failure: kafka_client_core::AdminDescribeConsumerGroupsFailure,
) -> DescribeConsumerGroupsFailure {
    DescribeConsumerGroupsFailure {
        kind: failure_kind(failure.kind()),
        delivery: delivery(failure.delivery()),
    }
}

fn description_from_core(
    description: kafka_client_core::AdminConsumerGroupDescription,
) -> ConsumerGroupDescription {
    let (state, details, members, operations) = description.into_parts();
    ConsumerGroupDescription::new(
        state,
        match details {
            CoreDescriptionDetails::Classic(details) => {
                let (protocol_type, protocol_data) = details.into_parts();
                ConsumerGroupDescriptionDetails::Classic(ClassicConsumerGroupDetails::new(
                    protocol_type,
                    protocol_data,
                ))
            }
            CoreDescriptionDetails::Consumer(details) => {
                let (group_epoch, assignment_epoch, assignor_name) = details.into_parts();
                ConsumerGroupDescriptionDetails::Consumer(ModernConsumerGroupDetails::new(
                    group_epoch,
                    assignment_epoch,
                    assignor_name,
                ))
            }
        },
        members.into_iter().map(member_from_core).collect(),
        operations,
    )
}

fn member_from_core(
    member: kafka_client_core::AdminConsumerGroupDescriptionMember,
) -> ConsumerGroupDescriptionMember {
    let (member_id, instance_id, client_id, client_host, details) = member.into_parts();
    ConsumerGroupDescriptionMember::new(
        member_id,
        instance_id,
        client_id,
        client_host,
        match details {
            CoreMemberDetails::Classic(details) => {
                let (metadata, assignment) = details.into_parts();
                ConsumerGroupMemberDetails::Classic(ClassicConsumerGroupMemberDetails::new(
                    metadata, assignment,
                ))
            }
            CoreMemberDetails::Consumer(details) => {
                let (
                    rack_id,
                    member_epoch,
                    subscriptions,
                    subscription_regex,
                    assignment,
                    target_assignment,
                    member_type,
                ) = details.into_parts();
                ConsumerGroupMemberDetails::Consumer(ModernConsumerGroupMemberDetails::new(
                    rack_id,
                    member_epoch,
                    subscriptions,
                    subscription_regex,
                    assignment_from_core(assignment),
                    assignment_from_core(target_assignment),
                    member_type,
                ))
            }
        },
    )
}

fn assignment_from_core(
    assignment: kafka_client_core::AdminConsumerGroupAssignment,
) -> ConsumerGroupAssignment {
    ConsumerGroupAssignment::new(
        assignment
            .into_topics()
            .into_iter()
            .map(|topic| {
                let (topic_id, topic_name, partitions) = topic.into_parts();
                ConsumerGroupTopicPartitions::new(topic_id, topic_name, partitions)
            })
            .collect(),
    )
}

const fn delivery(status: CoreDeliveryStatus) -> DescribeConsumerGroupsDeliveryStatus {
    match status {
        CoreDeliveryStatus::NotSent => DescribeConsumerGroupsDeliveryStatus::NotSent,
        CoreDeliveryStatus::PossiblySent => DescribeConsumerGroupsDeliveryStatus::PossiblySent,
    }
}
