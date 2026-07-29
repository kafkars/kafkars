//! Exhaustive core-to-engine API-77 terminal translation.

use kafka_client_core::{
    DeliveryStatus as CoreDeliveryStatus, DescribeShareGroupFailureKind as CoreFailureKind,
    DescribeShareGroupTerminal as CoreTerminal,
};

use super::{
    DescribeShareGroupBatchOutcome, DescribeShareGroupBrokerError,
    DescribeShareGroupDeliveryStatus, DescribeShareGroupFailure, DescribeShareGroupFailureKind,
    DescribeShareGroupOutcome, DescribeShareGroupsBatch,
};
use crate::admin::describe_share_group::{
    DescribeShareGroupAssignment, DescribeShareGroupDescription, DescribeShareGroupMember,
    DescribeShareGroupResult, DescribeShareGroupTopicAssignment,
};

pub(crate) fn translate_terminal(terminal: CoreTerminal) -> DescribeShareGroupOutcome {
    match terminal {
        CoreTerminal::Described(result) => {
            DescribeShareGroupOutcome::Described(translate_result(result))
        }
        CoreTerminal::BrokerRejected(error) => {
            DescribeShareGroupOutcome::BrokerRejected(translate_broker_error(error))
        }
        CoreTerminal::Batch(batch) => {
            let (throttle_time_ms, outcomes) = batch.into_parts();
            DescribeShareGroupOutcome::Batch(DescribeShareGroupsBatch {
                throttle_time_ms,
                outcomes: outcomes.into_iter().map(translate_batch_outcome).collect(),
            })
        }
        CoreTerminal::Failed(failure) => {
            DescribeShareGroupOutcome::Failed(DescribeShareGroupFailure {
                kind: failure_kind(failure.kind()),
                delivery: delivery(failure.delivery()),
            })
        }
    }
}

fn translate_batch_outcome(
    outcome: kafka_client_core::DescribeShareGroupOutcome,
) -> DescribeShareGroupBatchOutcome {
    match outcome {
        kafka_client_core::DescribeShareGroupOutcome::Described(result) => {
            DescribeShareGroupBatchOutcome::Described(translate_result(result))
        }
        kafka_client_core::DescribeShareGroupOutcome::BrokerRejected { group_id, error } => {
            DescribeShareGroupBatchOutcome::BrokerRejected {
                group_id,
                error: translate_broker_error(error),
            }
        }
    }
}

fn translate_result(
    result: kafka_client_core::DescribeShareGroupResult,
) -> DescribeShareGroupResult {
    let (throttle_time_ms, description) = result.into_parts();
    DescribeShareGroupResult {
        throttle_time_ms,
        description: translate_description(description),
    }
}

fn translate_broker_error(
    error: kafka_client_core::DescribeShareGroupBrokerError,
) -> DescribeShareGroupBrokerError {
    let (throttle_time_ms, code, message, message_truncated) = error.into_parts();
    DescribeShareGroupBrokerError {
        throttle_time_ms,
        code,
        message,
        message_truncated,
    }
}

fn translate_description(
    description: kafka_client_core::DescribeShareGroupDescription,
) -> DescribeShareGroupDescription {
    let (group_id, state, group_epoch, assignment_epoch, assignor_name, members, operations) =
        description.into_parts();
    DescribeShareGroupDescription {
        group_id,
        state,
        group_epoch,
        assignment_epoch,
        assignor_name,
        members: members.into_iter().map(translate_member).collect(),
        authorized_operations: operations,
    }
}

fn translate_member(
    member: kafka_client_core::DescribeShareGroupMember,
) -> DescribeShareGroupMember {
    let (member_id, rack_id, member_epoch, client_id, client_host, subscriptions, assignment) =
        member.into_parts();
    DescribeShareGroupMember {
        member_id,
        rack_id,
        member_epoch,
        client_id,
        client_host,
        subscribed_topic_names: subscriptions,
        assignment: DescribeShareGroupAssignment {
            topics: assignment
                .into_topics()
                .into_iter()
                .map(|topic| {
                    let (topic_id, topic_name, partitions) = topic.into_parts();
                    DescribeShareGroupTopicAssignment {
                        topic_id,
                        topic_name,
                        partitions,
                    }
                })
                .collect(),
        },
    }
}

const fn failure_kind(kind: CoreFailureKind) -> DescribeShareGroupFailureKind {
    match kind {
        CoreFailureKind::DeadlineElapsed => DescribeShareGroupFailureKind::DeadlineElapsed,
        CoreFailureKind::DriverRejected => DescribeShareGroupFailureKind::DriverRejected,
        CoreFailureKind::Transport => DescribeShareGroupFailureKind::Transport,
        CoreFailureKind::ResponseTooLarge => DescribeShareGroupFailureKind::ResponseTooLarge,
        CoreFailureKind::Compatibility => DescribeShareGroupFailureKind::Compatibility,
        CoreFailureKind::InvalidResponse => DescribeShareGroupFailureKind::InvalidResponse,
    }
}

const fn delivery(status: CoreDeliveryStatus) -> DescribeShareGroupDeliveryStatus {
    match status {
        CoreDeliveryStatus::NotSent => DescribeShareGroupDeliveryStatus::NotSent,
        CoreDeliveryStatus::PossiblySent => DescribeShareGroupDeliveryStatus::PossiblySent,
    }
}
