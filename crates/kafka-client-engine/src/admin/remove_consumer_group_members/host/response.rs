//! Translation from raw `LeaveGroup` facts into deterministic core input.

use kafka_client_core::{
    DeliveryStatus, RemoveConsumerGroupMembersInput, RemoveConsumerGroupMembersPlan,
};

use crate::{
    driver::{
        RemoveConsumerGroupMembersDriverFailureKind, RemoveConsumerGroupMembersTerminal,
        RemoveConsumerGroupMembersTerminalFact,
    },
    protocol::admin::remove_consumer_group_members::{
        RemoveConsumerGroupMembersProtocolFailure, ValidatedRemoveConsumerGroupMembersResponse,
        validate_remove_consumer_group_members_response,
    },
};

pub(super) fn terminal_input(
    terminal: &RemoveConsumerGroupMembersTerminal,
    plan: &RemoveConsumerGroupMembersPlan,
    result_limit: usize,
) -> RemoveConsumerGroupMembersInput {
    match terminal.fact() {
        RemoveConsumerGroupMembersTerminalFact::Failed { kind, delivery } => match kind {
            RemoveConsumerGroupMembersDriverFailureKind::DeadlineElapsed => {
                RemoveConsumerGroupMembersInput::DriverDeadlineElapsed { delivery }
            }
            RemoveConsumerGroupMembersDriverFailureKind::Compatibility => {
                RemoveConsumerGroupMembersInput::ProtocolIncompatible { delivery }
            }
            RemoveConsumerGroupMembersDriverFailureKind::InvalidResponse => {
                RemoveConsumerGroupMembersInput::InvalidResponse
            }
            RemoveConsumerGroupMembersDriverFailureKind::Transport => {
                RemoveConsumerGroupMembersInput::TransportFailed { delivery }
            }
        },
        RemoveConsumerGroupMembersTerminalFact::Response {
            selected_version: None,
            ..
        } => RemoveConsumerGroupMembersInput::InvalidResponse,
        RemoveConsumerGroupMembersTerminalFact::Response {
            selected_version: Some(selected_version),
            response,
        } => match validate_remove_consumer_group_members_response(
            plan,
            response,
            selected_version,
            result_limit,
        ) {
            Ok(ValidatedRemoveConsumerGroupMembersResponse::BrokerRejected(code)) => {
                RemoveConsumerGroupMembersInput::BrokerRejected { code }
            }
            Ok(ValidatedRemoveConsumerGroupMembersResponse::Batch(batch)) => {
                RemoveConsumerGroupMembersInput::BrokerResponded { batch }
            }
            Err(error) => protocol_failure(error),
        },
    }
}

const fn protocol_failure(
    error: RemoveConsumerGroupMembersProtocolFailure,
) -> RemoveConsumerGroupMembersInput {
    match error {
        RemoveConsumerGroupMembersProtocolFailure::UnsupportedApiVersion { .. } => {
            RemoveConsumerGroupMembersInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            }
        }
        RemoveConsumerGroupMembersProtocolFailure::RetainedBytes => {
            RemoveConsumerGroupMembersInput::ResponseTooLarge
        }
        _ => RemoveConsumerGroupMembersInput::InvalidResponse,
    }
}
