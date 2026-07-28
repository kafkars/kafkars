//! Exhaustive translation from borrowed wire facts into deterministic core input.

use kafka_client_core::{DeleteConsumerGroupsInput, DeliveryStatus};

use crate::{
    driver::{
        DeleteConsumerGroupsDriverFailureKind, DeleteConsumerGroupsRawTerminal,
        DeleteConsumerGroupsTerminalFact,
    },
    protocol::admin::delete_groups::{
        DeleteConsumerGroupsResponseFailure, normalize_delete_consumer_groups_response,
    },
};

pub(super) fn terminal_input(
    terminal: &DeleteConsumerGroupsRawTerminal,
) -> (DeleteConsumerGroupsInput, usize) {
    match terminal.fact() {
        DeleteConsumerGroupsTerminalFact::Failed { kind, delivery } => match kind {
            DeleteConsumerGroupsDriverFailureKind::DeadlineElapsed => (
                DeleteConsumerGroupsInput::DriverDeadlineElapsed { delivery },
                0,
            ),
            DeleteConsumerGroupsDriverFailureKind::Compatibility => (
                DeleteConsumerGroupsInput::ProtocolIncompatible { delivery },
                0,
            ),
            DeleteConsumerGroupsDriverFailureKind::InvalidResponse => {
                (DeleteConsumerGroupsInput::InvalidResponse, 0)
            }
            DeleteConsumerGroupsDriverFailureKind::Transport => {
                (DeleteConsumerGroupsInput::TransportFailed { delivery }, 0)
            }
        },
        DeleteConsumerGroupsTerminalFact::Response {
            selected_version: None,
            ..
        } => (
            DeleteConsumerGroupsInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            },
            0,
        ),
        DeleteConsumerGroupsTerminalFact::Response {
            selected_version: Some(selected_version),
            response,
        } => match normalize_delete_consumer_groups_response(
            terminal.response_target(),
            selected_version,
            response,
            terminal.result_limit(),
        ) {
            Ok(normalized) => {
                let (throttle_time_ms, outcome, retained_bytes) = normalized.into_parts();
                (
                    DeleteConsumerGroupsInput::BrokerResponded {
                        throttle_time_ms,
                        outcome,
                    },
                    retained_bytes,
                )
            }
            Err(DeleteConsumerGroupsResponseFailure::UnsupportedApiVersion { .. }) => (
                DeleteConsumerGroupsInput::ProtocolIncompatible {
                    delivery: DeliveryStatus::PossiblySent,
                },
                0,
            ),
            Err(DeleteConsumerGroupsResponseFailure::RetainedBytes) => {
                (DeleteConsumerGroupsInput::ResponseTooLarge, 0)
            }
            Err(
                DeleteConsumerGroupsResponseFailure::NegativeThrottleTime { .. }
                | DeleteConsumerGroupsResponseFailure::MissingGroup
                | DeleteConsumerGroupsResponseFailure::DuplicateGroup
                | DeleteConsumerGroupsResponseFailure::UnexpectedGroup,
            ) => (DeleteConsumerGroupsInput::InvalidResponse, 0),
        },
    }
}
