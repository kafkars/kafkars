//! Translation from raw driver and generated response facts into core input.

use kafka_client_core::{
    AlterPartitionReassignmentsInput, AlterPartitionReassignmentsPlan, DeliveryStatus,
};

use crate::{
    driver::{
        AlterPartitionReassignmentsDriverFailureKind, AlterPartitionReassignmentsTerminal,
        AlterPartitionReassignmentsTerminalFact,
    },
    protocol::admin::alter_partition_reassignments::{
        AlterPartitionReassignmentRef, AlterPartitionReassignmentsProtocolFailure,
        ValidatedAlterPartitionReassignmentsResponse,
        validate_alter_partition_reassignments_response,
    },
};

pub(super) fn terminal_input(
    terminal: &AlterPartitionReassignmentsTerminal,
) -> AlterPartitionReassignmentsInput {
    let plan = terminal.response_plan();
    let result_limit = terminal.result_limit();
    match terminal.fact() {
        AlterPartitionReassignmentsTerminalFact::Failed { kind, delivery } => match kind {
            AlterPartitionReassignmentsDriverFailureKind::DeadlineElapsed => {
                AlterPartitionReassignmentsInput::DriverDeadlineElapsed { delivery }
            }
            AlterPartitionReassignmentsDriverFailureKind::Compatibility => {
                AlterPartitionReassignmentsInput::ProtocolIncompatible { delivery }
            }
            AlterPartitionReassignmentsDriverFailureKind::InvalidResponse => {
                AlterPartitionReassignmentsInput::InvalidResponse
            }
            AlterPartitionReassignmentsDriverFailureKind::Transport => {
                AlterPartitionReassignmentsInput::TransportFailed { delivery }
            }
        },
        AlterPartitionReassignmentsTerminalFact::Response {
            selected_version: None,
            ..
        } => AlterPartitionReassignmentsInput::InvalidResponse,
        AlterPartitionReassignmentsTerminalFact::Response {
            selected_version: Some(selected_version),
            response,
        } => {
            let changes = match change_refs(plan) {
                Ok(changes) => changes,
                Err(input) => return input,
            };
            match validate_alter_partition_reassignments_response(
                &changes,
                plan.allow_replication_factor_change(),
                response,
                selected_version,
                result_limit,
            ) {
                Ok(ValidatedAlterPartitionReassignmentsResponse::BrokerRejected(error)) => {
                    AlterPartitionReassignmentsInput::BrokerRejected { error }
                }
                Ok(ValidatedAlterPartitionReassignmentsResponse::Batch(batch)) => {
                    AlterPartitionReassignmentsInput::BrokerResponded { batch }
                }
                Err(error) => protocol_failure(error),
            }
        }
    }
}

fn change_refs(
    plan: &AlterPartitionReassignmentsPlan,
) -> Result<Vec<AlterPartitionReassignmentRef<'_>>, AlterPartitionReassignmentsInput> {
    let mut changes = Vec::new();
    changes
        .try_reserve_exact(plan.changes().len())
        .map_err(|_error| AlterPartitionReassignmentsInput::ResponseTooLarge)?;
    changes.extend(plan.changes().iter().map(|change| {
        AlterPartitionReassignmentRef::new(
            change.topic(),
            change.partition(),
            change.target().replicas(),
        )
    }));
    Ok(changes)
}

const fn protocol_failure(
    error: AlterPartitionReassignmentsProtocolFailure,
) -> AlterPartitionReassignmentsInput {
    match error {
        AlterPartitionReassignmentsProtocolFailure::UnsupportedApiVersion { .. } => {
            AlterPartitionReassignmentsInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            }
        }
        AlterPartitionReassignmentsProtocolFailure::RetainedBytes => {
            AlterPartitionReassignmentsInput::ResponseTooLarge
        }
        _ => AlterPartitionReassignmentsInput::InvalidResponse,
    }
}
