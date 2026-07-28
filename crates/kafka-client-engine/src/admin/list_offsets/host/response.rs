//! Exhaustive translation from borrowed wire facts into deterministic core input.

use kafka_client_core::{
    AdminListOffsetTarget, AdminListOffsetsInput, DeliveryStatus, ReadIsolation,
};

use crate::{
    driver::{
        AdminListOffsetsDriverFailureKind, AdminListOffsetsTerminal, AdminListOffsetsTerminalFact,
    },
    protocol::admin::list_offsets::{
        AdminListOffsetsResponseFailure, normalize_admin_list_offsets_response,
    },
};

pub(super) fn terminal_input(
    terminal: &AdminListOffsetsTerminal,
    target: &AdminListOffsetTarget,
    read_isolation: ReadIsolation,
) -> AdminListOffsetsInput {
    match terminal.fact() {
        AdminListOffsetsTerminalFact::Failed { kind, delivery } => match kind {
            AdminListOffsetsDriverFailureKind::DeadlineElapsed => {
                AdminListOffsetsInput::DriverDeadlineElapsed { delivery }
            }
            AdminListOffsetsDriverFailureKind::Compatibility => {
                AdminListOffsetsInput::ProtocolIncompatible { delivery }
            }
            AdminListOffsetsDriverFailureKind::InvalidResponse => {
                AdminListOffsetsInput::InvalidResponse
            }
            AdminListOffsetsDriverFailureKind::Transport => {
                AdminListOffsetsInput::TransportFailed { delivery }
            }
        },
        AdminListOffsetsTerminalFact::Response {
            selected_version: None,
            ..
        } => AdminListOffsetsInput::ProtocolIncompatible {
            delivery: DeliveryStatus::PossiblySent,
        },
        AdminListOffsetsTerminalFact::Response {
            selected_version: Some(selected_version),
            response,
        } => match normalize_admin_list_offsets_response(
            target,
            read_isolation,
            selected_version,
            response,
        ) {
            Ok(normalized) => {
                let (throttle_time_ms, outcome) = normalized.into_parts();
                AdminListOffsetsInput::BrokerResponded {
                    throttle_time_ms,
                    outcome,
                }
            }
            Err(AdminListOffsetsResponseFailure::UnsupportedApiVersion { .. }) => {
                AdminListOffsetsInput::ProtocolIncompatible {
                    delivery: DeliveryStatus::PossiblySent,
                }
            }
            Err(
                AdminListOffsetsResponseFailure::NegativeThrottleTime { .. }
                | AdminListOffsetsResponseFailure::MissingTopic
                | AdminListOffsetsResponseFailure::DuplicateTopic
                | AdminListOffsetsResponseFailure::UnexpectedTopic
                | AdminListOffsetsResponseFailure::MissingPartition
                | AdminListOffsetsResponseFailure::DuplicatePartition
                | AdminListOffsetsResponseFailure::InvalidPartitionIndex { .. }
                | AdminListOffsetsResponseFailure::UnexpectedPartition { .. }
                | AdminListOffsetsResponseFailure::InvalidOffset { .. }
                | AdminListOffsetsResponseFailure::InvalidTimestamp { .. }
                | AdminListOffsetsResponseFailure::InvalidLeaderEpoch { .. },
            ) => AdminListOffsetsInput::InvalidResponse,
        },
    }
}
