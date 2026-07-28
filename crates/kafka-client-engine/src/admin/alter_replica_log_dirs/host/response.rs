//! Exhaustive raw-driver and protocol-terminal translation.

use core::num::NonZeroI16;

use kafka_client_core::{
    AlterReplicaLogDirAssignment, AlterReplicaLogDirBrokerError, AlterReplicaLogDirOutcome,
    AlterReplicaLogDirsInput, DeliveryStatus,
};

use crate::{
    driver::{
        AlterReplicaLogDirsDriverFailureKind, AlterReplicaLogDirsRawTerminal,
        AlterReplicaLogDirsTerminalFact,
    },
    protocol::admin::alter_replica_log_dirs::{
        AlterReplicaLogDirAssignmentRef, AlterReplicaLogDirsResponseFailure,
        NormalizedAlterReplicaLogDirsResponse, normalize_alter_replica_log_dirs_response,
    },
};

pub(super) fn terminal_input(
    raw: &AlterReplicaLogDirsRawTerminal,
) -> (AlterReplicaLogDirsInput, usize) {
    let assignments = raw.assignments();
    let Some(broker_id) = one_broker(assignments) else {
        return (AlterReplicaLogDirsInput::InvalidResponse, 0);
    };
    match raw.fact() {
        AlterReplicaLogDirsTerminalFact::Response {
            broker_id: response_broker,
            selected_version: Some(selected_version),
            response,
        } if response_broker == broker_id => {
            let Some(assignment_refs) = assignment_refs(assignments) else {
                return (AlterReplicaLogDirsInput::ResponseTooLarge, 0);
            };
            match normalize_alter_replica_log_dirs_response(
                &assignment_refs,
                selected_version,
                response,
                raw.result_limit(),
            ) {
                Ok(normalized) => normalized_input(broker_id, normalized)
                    .unwrap_or((AlterReplicaLogDirsInput::ResponseTooLarge, 0)),
                Err(AlterReplicaLogDirsResponseFailure::RetainedBytes { .. }) => {
                    (AlterReplicaLogDirsInput::ResponseTooLarge, 0)
                }
                Err(AlterReplicaLogDirsResponseFailure::UnsupportedApiVersion { .. }) => (
                    AlterReplicaLogDirsInput::ProtocolIncompatible {
                        delivery: DeliveryStatus::PossiblySent,
                    },
                    0,
                ),
                Err(_) => (AlterReplicaLogDirsInput::InvalidResponse, 0),
            }
        }
        AlterReplicaLogDirsTerminalFact::Response {
            selected_version: None,
            ..
        } => (
            AlterReplicaLogDirsInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            },
            0,
        ),
        AlterReplicaLogDirsTerminalFact::Response { .. } => {
            (AlterReplicaLogDirsInput::InvalidResponse, 0)
        }
        AlterReplicaLogDirsTerminalFact::Failed { kind, delivery } => {
            (driver_failure(kind, delivery), 0)
        }
    }
}

fn assignment_refs(
    assignments: &[AlterReplicaLogDirAssignment],
) -> Option<Vec<AlterReplicaLogDirAssignmentRef<'_>>> {
    let mut refs = Vec::new();
    refs.try_reserve_exact(assignments.len()).ok()?;
    for assignment in assignments {
        refs.push(AlterReplicaLogDirAssignmentRef::new(
            assignment.topic(),
            assignment.partition(),
            assignment.log_dir(),
        ));
    }
    Some(refs)
}

fn one_broker(assignments: &[AlterReplicaLogDirAssignment]) -> Option<i32> {
    let broker_id = assignments.first()?.broker_id();
    assignments
        .iter()
        .all(|assignment| assignment.broker_id() == broker_id)
        .then_some(broker_id)
}

pub(super) fn normalized_input(
    broker_id: i32,
    normalized: NormalizedAlterReplicaLogDirsResponse,
) -> Result<(AlterReplicaLogDirsInput, usize), ()> {
    let (_selected_version, throttle_time_ms, normalized_outcomes, retained_bytes) =
        normalized.into_parts();
    let mut outcomes = Vec::new();
    outcomes
        .try_reserve_exact(normalized_outcomes.len())
        .map_err(|_| ())?;
    for normalized in normalized_outcomes {
        let (topic, partition, error_code) = normalized.into_parts();
        let outcome = match NonZeroI16::new(error_code) {
            Some(code) => AlterReplicaLogDirOutcome::broker_failed(
                broker_id,
                topic,
                partition,
                AlterReplicaLogDirBrokerError::new(code),
            ),
            None => AlterReplicaLogDirOutcome::altered(broker_id, topic, partition),
        };
        outcomes.push(outcome);
    }
    Ok((
        AlterReplicaLogDirsInput::BrokerResponded {
            throttle_time_ms,
            outcomes,
        },
        retained_bytes,
    ))
}

const fn driver_failure(
    kind: AlterReplicaLogDirsDriverFailureKind,
    delivery: DeliveryStatus,
) -> AlterReplicaLogDirsInput {
    match kind {
        AlterReplicaLogDirsDriverFailureKind::DeadlineElapsed => {
            AlterReplicaLogDirsInput::DriverDeadlineElapsed { delivery }
        }
        AlterReplicaLogDirsDriverFailureKind::Compatibility => {
            AlterReplicaLogDirsInput::ProtocolIncompatible { delivery }
        }
        AlterReplicaLogDirsDriverFailureKind::InvalidResponse => {
            AlterReplicaLogDirsInput::InvalidResponse
        }
        AlterReplicaLogDirsDriverFailureKind::Transport => {
            AlterReplicaLogDirsInput::TransportFailed { delivery }
        }
    }
}
