//! Exhaustive raw-terminal translation into deterministic core input.

use kafka_client_core::{AdminDescribeConsumerGroupsInput, DeliveryStatus};

use crate::{
    driver::{
        ConsumerGroupDescribeDriverFailureKind, ConsumerGroupDescribeTerminalFact,
        DescribeConsumerGroupsDriverFailureKind, DescribeConsumerGroupsTerminal,
        DescribeConsumerGroupsTerminalFact,
    },
    protocol::admin::describe_consumer_groups::{
        ConsumerGroupDescribeResponseFailure, DescribeConsumerGroupResponseFailure,
        normalize_consumer_group_describe_response, normalize_describe_consumer_group_response,
    },
};

use super::modern_response::modern_outcome;

pub(super) fn terminal_input(
    terminal: &DescribeConsumerGroupsTerminal,
) -> (AdminDescribeConsumerGroupsInput, usize) {
    let group_id = terminal.group_id();
    let include_authorized_operations = terminal.include_authorized_operations();
    let retained_limit = terminal.result_limit();
    let input = match terminal.fact() {
        DescribeConsumerGroupsTerminalFact::Consumer(fact) => {
            return modern_terminal_input(
                &fact,
                group_id,
                include_authorized_operations,
                retained_limit,
            );
        }
        DescribeConsumerGroupsTerminalFact::ClassicFailed { kind, delivery } => match kind {
            DescribeConsumerGroupsDriverFailureKind::DeadlineElapsed => {
                AdminDescribeConsumerGroupsInput::DriverDeadlineElapsed { delivery }
            }
            DescribeConsumerGroupsDriverFailureKind::Compatibility => {
                AdminDescribeConsumerGroupsInput::ProtocolIncompatible { delivery }
            }
            DescribeConsumerGroupsDriverFailureKind::InvalidResponse => {
                AdminDescribeConsumerGroupsInput::InvalidResponse
            }
            DescribeConsumerGroupsDriverFailureKind::Transport => {
                AdminDescribeConsumerGroupsInput::TransportFailed { delivery }
            }
        },
        DescribeConsumerGroupsTerminalFact::ClassicResponse {
            selected_version: None,
            ..
        } => AdminDescribeConsumerGroupsInput::ProtocolIncompatible {
            delivery: DeliveryStatus::PossiblySent,
        },
        DescribeConsumerGroupsTerminalFact::ClassicResponse {
            selected_version: Some(selected_version),
            response,
        } => match normalize_describe_consumer_group_response(
            group_id,
            include_authorized_operations,
            selected_version,
            response,
            retained_limit,
        ) {
            Ok(normalized) => {
                let (throttle_time_ms, outcome, retained_bytes) = normalized.into_parts();
                return (
                    AdminDescribeConsumerGroupsInput::BrokerResponded {
                        throttle_time_ms,
                        outcome,
                    },
                    retained_bytes,
                );
            }
            Err(
                DescribeConsumerGroupResponseFailure::UnsupportedApiVersion { .. }
                | DescribeConsumerGroupResponseFailure::AuthorizedOperationsUnavailable { .. },
            ) => AdminDescribeConsumerGroupsInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            },
            Err(DescribeConsumerGroupResponseFailure::RetainedBytes) => {
                AdminDescribeConsumerGroupsInput::ResponseTooLarge
            }
            Err(
                DescribeConsumerGroupResponseFailure::NegativeThrottleTime { .. }
                | DescribeConsumerGroupResponseFailure::MissingGroup
                | DescribeConsumerGroupResponseFailure::DuplicateGroup
                | DescribeConsumerGroupResponseFailure::UnexpectedGroup
                | DescribeConsumerGroupResponseFailure::DuplicateMemberId,
            ) => AdminDescribeConsumerGroupsInput::InvalidResponse,
        },
    };
    (input, 0)
}

fn modern_terminal_input(
    fact: &ConsumerGroupDescribeTerminalFact<'_>,
    group_id: &str,
    include_authorized_operations: bool,
    retained_limit: usize,
) -> (AdminDescribeConsumerGroupsInput, usize) {
    let input = match fact {
        ConsumerGroupDescribeTerminalFact::Failed { kind, delivery } => match kind {
            ConsumerGroupDescribeDriverFailureKind::LocalApiUnavailable
            | ConsumerGroupDescribeDriverFailureKind::LocalUnsupportedVersion => {
                AdminDescribeConsumerGroupsInput::FallbackToClassic {
                    throttle_time_ms: 0,
                    delivery: *delivery,
                }
            }
            ConsumerGroupDescribeDriverFailureKind::DeadlineElapsed => {
                AdminDescribeConsumerGroupsInput::DriverDeadlineElapsed {
                    delivery: *delivery,
                }
            }
            ConsumerGroupDescribeDriverFailureKind::InvalidResponse => {
                AdminDescribeConsumerGroupsInput::InvalidResponse
            }
            ConsumerGroupDescribeDriverFailureKind::Transport => {
                AdminDescribeConsumerGroupsInput::TransportFailed {
                    delivery: *delivery,
                }
            }
        },
        ConsumerGroupDescribeTerminalFact::Response {
            selected_version: None,
            ..
        } => AdminDescribeConsumerGroupsInput::FallbackToClassic {
            throttle_time_ms: 0,
            delivery: DeliveryStatus::PossiblySent,
        },
        ConsumerGroupDescribeTerminalFact::Response {
            selected_version: Some(selected_version),
            response,
        } => match normalize_consumer_group_describe_response(
            group_id,
            include_authorized_operations,
            *selected_version,
            response,
            retained_limit,
        ) {
            Ok(normalized) => {
                let (throttle_time_ms, group_id, result, fallback, retained_bytes) =
                    normalized.into_parts();
                if fallback.is_some() {
                    return (
                        AdminDescribeConsumerGroupsInput::FallbackToClassic {
                            throttle_time_ms,
                            delivery: DeliveryStatus::PossiblySent,
                        },
                        0,
                    );
                }
                let Some(outcome) = modern_outcome(group_id, result) else {
                    return (AdminDescribeConsumerGroupsInput::InvalidResponse, 0);
                };
                return (
                    AdminDescribeConsumerGroupsInput::BrokerResponded {
                        throttle_time_ms,
                        outcome,
                    },
                    retained_bytes,
                );
            }
            Err(ConsumerGroupDescribeResponseFailure::LocalUnsupportedVersion { .. }) => {
                AdminDescribeConsumerGroupsInput::FallbackToClassic {
                    throttle_time_ms: 0,
                    delivery: DeliveryStatus::PossiblySent,
                }
            }
            Err(ConsumerGroupDescribeResponseFailure::ResponseTooLarge) => {
                AdminDescribeConsumerGroupsInput::ResponseTooLarge
            }
            Err(
                ConsumerGroupDescribeResponseFailure::NegativeThrottleTime { .. }
                | ConsumerGroupDescribeResponseFailure::MissingGroup
                | ConsumerGroupDescribeResponseFailure::DuplicateGroup
                | ConsumerGroupDescribeResponseFailure::UnexpectedGroup
                | ConsumerGroupDescribeResponseFailure::EmptyMemberId
                | ConsumerGroupDescribeResponseFailure::DuplicateMemberId
                | ConsumerGroupDescribeResponseFailure::EmptyInstanceId
                | ConsumerGroupDescribeResponseFailure::EmptySubscription
                | ConsumerGroupDescribeResponseFailure::DuplicateSubscription
                | ConsumerGroupDescribeResponseFailure::TopicId
                | ConsumerGroupDescribeResponseFailure::EmptyTopicName
                | ConsumerGroupDescribeResponseFailure::DuplicateTopicId
                | ConsumerGroupDescribeResponseFailure::DuplicateTopicName
                | ConsumerGroupDescribeResponseFailure::Partition
                | ConsumerGroupDescribeResponseFailure::DuplicatePartition
                | ConsumerGroupDescribeResponseFailure::ScalarTooLarge,
            ) => AdminDescribeConsumerGroupsInput::InvalidResponse,
        },
    };
    (input, 0)
}
