//! Exhaustive generated-free response and driver-failure translation.

use kafka_client_core::{AlterShareGroupOffsetsInput, AlterShareGroupOffsetsPlan, DeliveryStatus};

use crate::{
    driver::{
        AlterShareGroupOffsetsDriverFailureKind, AlterShareGroupOffsetsTerminal as DriverTerminal,
        AlterShareGroupOffsetsTerminalFact,
    },
    protocol::admin::alter_share_group_offsets::{
        AlterShareGroupOffsetsProtocolFailure, normalize_alter_share_group_offsets_response,
    },
};

pub(super) fn terminal_input(
    raw: &DriverTerminal,
    plan: &AlterShareGroupOffsetsPlan,
    retained_limit: usize,
) -> (AlterShareGroupOffsetsInput, usize) {
    match raw.fact() {
        AlterShareGroupOffsetsTerminalFact::Response {
            selected_version,
            response,
        } => match normalize_alter_share_group_offsets_response(
            plan,
            selected_version,
            response,
            retained_limit,
        ) {
            Ok(normalized) => {
                let (result, retained_bytes) = normalized.into_parts();
                let input = match result {
                    Ok(batch) => AlterShareGroupOffsetsInput::BrokerResponded { batch },
                    Err(error) => AlterShareGroupOffsetsInput::BrokerRejected { error },
                };
                (input, retained_bytes)
            }
            Err(error) => (protocol_failure(error), 0),
        },
        AlterShareGroupOffsetsTerminalFact::Failed { kind, delivery } => {
            (driver_failure(kind, delivery), 0)
        }
    }
}

pub(super) const fn protocol_failure(
    error: AlterShareGroupOffsetsProtocolFailure,
) -> AlterShareGroupOffsetsInput {
    match error {
        AlterShareGroupOffsetsProtocolFailure::MissingSelectedVersion
        | AlterShareGroupOffsetsProtocolFailure::UnsupportedApiVersion { .. } => {
            AlterShareGroupOffsetsInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            }
        }
        AlterShareGroupOffsetsProtocolFailure::NormalizedBytesExceeded { .. }
        | AlterShareGroupOffsetsProtocolFailure::RetainedBytes { .. }
        | AlterShareGroupOffsetsProtocolFailure::Allocation { .. }
        | AlterShareGroupOffsetsProtocolFailure::TooManyTopics { .. }
        | AlterShareGroupOffsetsProtocolFailure::TooManyPartitions { .. }
        | AlterShareGroupOffsetsProtocolFailure::TopicNameTooLong { .. }
        | AlterShareGroupOffsetsProtocolFailure::ResponseTextBytesExceeded { .. } => {
            AlterShareGroupOffsetsInput::ResponseTooLarge
        }
        AlterShareGroupOffsetsProtocolFailure::NegativeThrottleTime { .. }
        | AlterShareGroupOffsetsProtocolFailure::EmptyTopicName
        | AlterShareGroupOffsetsProtocolFailure::EmptyTopicPartitions
        | AlterShareGroupOffsetsProtocolFailure::NegativePartition { .. }
        | AlterShareGroupOffsetsProtocolFailure::DuplicateTopic
        | AlterShareGroupOffsetsProtocolFailure::DuplicatePartition { .. }
        | AlterShareGroupOffsetsProtocolFailure::MissingPartition
        | AlterShareGroupOffsetsProtocolFailure::UnexpectedPartition
        | AlterShareGroupOffsetsProtocolFailure::DiagnosticOnSuccess
        | AlterShareGroupOffsetsProtocolFailure::ZeroTopicId
        | AlterShareGroupOffsetsProtocolFailure::PartitionsOnTopLevelError => {
            AlterShareGroupOffsetsInput::InvalidResponse
        }
    }
}

const fn driver_failure(
    kind: AlterShareGroupOffsetsDriverFailureKind,
    delivery: DeliveryStatus,
) -> AlterShareGroupOffsetsInput {
    match kind {
        AlterShareGroupOffsetsDriverFailureKind::DeadlineElapsed => {
            AlterShareGroupOffsetsInput::DriverDeadlineElapsed { delivery }
        }
        AlterShareGroupOffsetsDriverFailureKind::Compatibility => {
            AlterShareGroupOffsetsInput::ProtocolIncompatible { delivery }
        }
        AlterShareGroupOffsetsDriverFailureKind::InvalidResponse => {
            AlterShareGroupOffsetsInput::InvalidResponse
        }
        AlterShareGroupOffsetsDriverFailureKind::Transport => {
            AlterShareGroupOffsetsInput::TransportFailed { delivery }
        }
    }
}
