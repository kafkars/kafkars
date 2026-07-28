//! Exhaustive translation from driver and wire facts into deterministic core input.

use kafka_client_core::{
    DeliveryStatus, ListPartitionReassignmentsInput, ListPartitionReassignmentsPlan,
};

use crate::{
    driver::{
        ListPartitionReassignmentsDriverFailureKind, ListPartitionReassignmentsRawTerminal,
        ListPartitionReassignmentsTerminalFact,
    },
    protocol::admin::list_partition_reassignments::{
        ListPartitionReassignmentsProtocolFailure, normalize_list_partition_reassignments_response,
    },
};

pub(super) fn terminal_input(
    terminal: &ListPartitionReassignmentsRawTerminal,
    plan: &ListPartitionReassignmentsPlan,
    result_limit: usize,
) -> ListPartitionReassignmentsInput {
    match terminal.fact() {
        ListPartitionReassignmentsTerminalFact::Failed { kind, delivery } => match kind {
            ListPartitionReassignmentsDriverFailureKind::DeadlineElapsed => {
                ListPartitionReassignmentsInput::DriverDeadlineElapsed { delivery }
            }
            ListPartitionReassignmentsDriverFailureKind::Compatibility => {
                ListPartitionReassignmentsInput::ProtocolIncompatible { delivery }
            }
            ListPartitionReassignmentsDriverFailureKind::InvalidResponse => {
                ListPartitionReassignmentsInput::InvalidResponse
            }
            ListPartitionReassignmentsDriverFailureKind::Transport => {
                ListPartitionReassignmentsInput::TransportFailed { delivery }
            }
        },
        ListPartitionReassignmentsTerminalFact::Response {
            selected_version: None,
            ..
        } => ListPartitionReassignmentsInput::InvalidResponse,
        ListPartitionReassignmentsTerminalFact::Response {
            selected_version: Some(selected_version),
            response,
        } => match normalize_list_partition_reassignments_response(
            plan,
            response,
            selected_version,
            result_limit,
        ) {
            Ok(input) => input,
            Err(error) => protocol_failure(error),
        },
    }
}

const fn protocol_failure(
    error: ListPartitionReassignmentsProtocolFailure,
) -> ListPartitionReassignmentsInput {
    match error {
        ListPartitionReassignmentsProtocolFailure::UnsupportedApiVersion { .. } => {
            ListPartitionReassignmentsInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            }
        }
        ListPartitionReassignmentsProtocolFailure::RetainedBytes => {
            ListPartitionReassignmentsInput::ResponseTooLarge
        }
        _ => ListPartitionReassignmentsInput::InvalidResponse,
    }
}
