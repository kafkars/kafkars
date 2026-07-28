//! Exhaustive translation from borrowed wire facts into deterministic core input.

use kafka_client_core::{DeleteRecordsInput, DeleteRecordsTarget, DeliveryStatus};

use crate::{
    driver::{DeleteRecordsDriverFailureKind, DeleteRecordsRawTerminal, DeleteRecordsTerminalFact},
    protocol::admin::delete_records::{
        DeleteRecordsResponseFailure, normalize_delete_records_response,
    },
};

pub(super) fn terminal_input(
    terminal: &DeleteRecordsRawTerminal,
    target: &DeleteRecordsTarget,
) -> DeleteRecordsInput {
    match terminal.fact() {
        DeleteRecordsTerminalFact::Failed { kind, delivery } => match kind {
            DeleteRecordsDriverFailureKind::DeadlineElapsed => {
                DeleteRecordsInput::DriverDeadlineElapsed { delivery }
            }
            DeleteRecordsDriverFailureKind::Compatibility => {
                DeleteRecordsInput::ProtocolIncompatible { delivery }
            }
            DeleteRecordsDriverFailureKind::InvalidResponse => DeleteRecordsInput::InvalidResponse,
            DeleteRecordsDriverFailureKind::Transport => {
                DeleteRecordsInput::TransportFailed { delivery }
            }
        },
        DeleteRecordsTerminalFact::Response {
            selected_version: None,
            ..
        } => DeleteRecordsInput::ProtocolIncompatible {
            delivery: DeliveryStatus::PossiblySent,
        },
        DeleteRecordsTerminalFact::Response {
            selected_version: Some(selected_version),
            response,
        } => match normalize_delete_records_response(target, selected_version, response) {
            Ok(normalized) => {
                let (throttle_time_ms, outcome) = normalized.into_parts();
                DeleteRecordsInput::BrokerResponded {
                    throttle_time_ms,
                    outcome,
                }
            }
            Err(DeleteRecordsResponseFailure::UnsupportedApiVersion { .. }) => {
                DeleteRecordsInput::ProtocolIncompatible {
                    delivery: DeliveryStatus::PossiblySent,
                }
            }
            Err(
                DeleteRecordsResponseFailure::NegativeThrottleTime { .. }
                | DeleteRecordsResponseFailure::MissingTopic
                | DeleteRecordsResponseFailure::DuplicateTopic
                | DeleteRecordsResponseFailure::UnexpectedTopic
                | DeleteRecordsResponseFailure::MissingPartition
                | DeleteRecordsResponseFailure::DuplicatePartition
                | DeleteRecordsResponseFailure::InvalidPartitionIndex { .. }
                | DeleteRecordsResponseFailure::UnexpectedPartition { .. }
                | DeleteRecordsResponseFailure::InvalidLowWatermark { .. },
            ) => DeleteRecordsInput::InvalidResponse,
        },
    }
}
