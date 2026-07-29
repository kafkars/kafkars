//! Exhaustive generated-free response and driver-failure translation.

use kafka_client_core::{DeliveryStatus, DescribeShareGroupInput, DescribeShareGroupPlan};

use crate::{
    driver::{
        DescribeShareGroupDriverFailureKind, DescribeShareGroupTerminal as DriverTerminal,
        DescribeShareGroupTerminalFact,
    },
    protocol::admin::describe_share_group::{
        DescribeShareGroupProtocolFailure, normalize_describe_share_group_response,
    },
};

pub(super) fn terminal_input(
    raw: &DriverTerminal,
    plan: &DescribeShareGroupPlan,
    retained_limit: usize,
) -> (DescribeShareGroupInput, usize) {
    match raw.fact() {
        DescribeShareGroupTerminalFact::Response {
            selected_version,
            response,
        } => match normalize_describe_share_group_response(
            plan.group_id(),
            plan.include_authorized_operations(),
            selected_version,
            response,
            retained_limit,
        ) {
            Ok(normalized) => {
                let (throttle, group_id, result, retained_bytes) = normalized.into_parts();
                let input = match result {
                    crate::protocol::admin::describe_share_group::DescribeShareGroupResult::Described(
                        description,
                    ) => DescribeShareGroupInput::BrokerResponded {
                        result: core_result(throttle, group_id, description),
                    },
                    crate::protocol::admin::describe_share_group::DescribeShareGroupResult::Failed(
                        error,
                    ) => {
                        let (code, message, truncated) = error.into_parts();
                        let Some(code) = core::num::NonZeroI16::new(code) else {
                            return (DescribeShareGroupInput::InvalidResponse, 0);
                        };
                        DescribeShareGroupInput::BrokerRejected {
                            error: kafka_client_core::DescribeShareGroupBrokerError::new(
                                throttle, code, message, truncated,
                            ),
                        }
                    }
                };
                (input, retained_bytes)
            }
            Err(error) => (protocol_failure(error), 0),
        },
        DescribeShareGroupTerminalFact::Failed { kind, delivery } => {
            (driver_failure(kind, delivery), 0)
        }
    }
}

pub(super) const fn protocol_failure(
    error: DescribeShareGroupProtocolFailure,
) -> DescribeShareGroupInput {
    match error {
        DescribeShareGroupProtocolFailure::MissingSelectedVersion
        | DescribeShareGroupProtocolFailure::UnsupportedApiVersion { .. } => {
            DescribeShareGroupInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            }
        }
        DescribeShareGroupProtocolFailure::RetainedBytes { .. }
        | DescribeShareGroupProtocolFailure::RetainedBytesOverflow
        | DescribeShareGroupProtocolFailure::Allocation
        | DescribeShareGroupProtocolFailure::TooManyMembers
        | DescribeShareGroupProtocolFailure::TooManySubscriptions
        | DescribeShareGroupProtocolFailure::TooManyAssignmentTopics
        | DescribeShareGroupProtocolFailure::TooManyPartitions
        | DescribeShareGroupProtocolFailure::ScalarTooLarge
        | DescribeShareGroupProtocolFailure::ResponseTextBytesExceeded
        | DescribeShareGroupProtocolFailure::GroupDiagnosticTooLarge => {
            DescribeShareGroupInput::ResponseTooLarge
        }
        DescribeShareGroupProtocolFailure::NegativeThrottleTime { .. }
        | DescribeShareGroupProtocolFailure::MissingGroup
        | DescribeShareGroupProtocolFailure::DuplicateGroup
        | DescribeShareGroupProtocolFailure::UnexpectedGroup
        | DescribeShareGroupProtocolFailure::DiagnosticOnSuccess
        | DescribeShareGroupProtocolFailure::MembersOnGroupError
        | DescribeShareGroupProtocolFailure::EmptyGroupState
        | DescribeShareGroupProtocolFailure::NegativeGroupEpoch
        | DescribeShareGroupProtocolFailure::UnexpectedAuthorizedOperations
        | DescribeShareGroupProtocolFailure::EmptyMemberId
        | DescribeShareGroupProtocolFailure::DuplicateMemberId
        | DescribeShareGroupProtocolFailure::EmptyRackId
        | DescribeShareGroupProtocolFailure::NegativeMemberEpoch
        | DescribeShareGroupProtocolFailure::EmptySubscription
        | DescribeShareGroupProtocolFailure::DuplicateSubscription
        | DescribeShareGroupProtocolFailure::ZeroTopicId
        | DescribeShareGroupProtocolFailure::EmptyTopicName
        | DescribeShareGroupProtocolFailure::DuplicateTopicId
        | DescribeShareGroupProtocolFailure::DuplicateTopicName
        | DescribeShareGroupProtocolFailure::NegativePartition
        | DescribeShareGroupProtocolFailure::DuplicatePartition => {
            DescribeShareGroupInput::InvalidResponse
        }
    }
}

fn core_result(
    throttle: u32,
    group_id: String,
    description: crate::protocol::admin::describe_share_group::DescribeShareGroupDescription,
) -> kafka_client_core::DescribeShareGroupResult {
    let (state, group_epoch, assignment_epoch, assignor, members, operations) =
        description.into_parts();
    kafka_client_core::DescribeShareGroupResult::new(
        throttle,
        kafka_client_core::DescribeShareGroupDescription::new(
            group_id,
            state,
            group_epoch,
            assignment_epoch,
            assignor,
            members.into_iter().map(core_member).collect(),
            operations,
        ),
    )
}

fn core_member(
    member: crate::protocol::admin::describe_share_group::DescribeShareGroupMember,
) -> kafka_client_core::DescribeShareGroupMember {
    let (member_id, rack_id, epoch, client_id, host, subscriptions, assignment) =
        member.into_parts();
    kafka_client_core::DescribeShareGroupMember::new(
        member_id,
        rack_id,
        epoch,
        client_id,
        host,
        subscriptions,
        kafka_client_core::DescribeShareGroupAssignment::new(
            assignment
                .into_topics()
                .into_iter()
                .map(|topic| {
                    let (id, name, partitions) = topic.into_parts();
                    kafka_client_core::DescribeShareGroupTopicAssignment::new(id, name, partitions)
                })
                .collect(),
        ),
    )
}

const fn driver_failure(
    kind: DescribeShareGroupDriverFailureKind,
    delivery: DeliveryStatus,
) -> DescribeShareGroupInput {
    match kind {
        DescribeShareGroupDriverFailureKind::DeadlineElapsed => {
            DescribeShareGroupInput::DriverDeadlineElapsed { delivery }
        }
        DescribeShareGroupDriverFailureKind::Compatibility => {
            DescribeShareGroupInput::ProtocolIncompatible { delivery }
        }
        DescribeShareGroupDriverFailureKind::InvalidResponse => {
            DescribeShareGroupInput::InvalidResponse
        }
        DescribeShareGroupDriverFailureKind::Transport => {
            DescribeShareGroupInput::TransportFailed { delivery }
        }
    }
}
