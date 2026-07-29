//! Exhaustive generated-free response and driver-failure translation.

use kafka_client_core::{
    DeleteShareGroupOffsetsInput, DeleteShareGroupOffsetsPlan, DeliveryStatus,
};

use crate::{
    driver::{
        DeleteShareGroupOffsetsDriverFailureKind,
        DeleteShareGroupOffsetsTerminal as DriverTerminal, DeleteShareGroupOffsetsTerminalFact,
    },
    protocol::admin::delete_share_group_offsets::{
        DeleteShareGroupOffsetsProtocolFailure, normalize_delete_share_group_offsets_response,
    },
};

pub(super) fn terminal_input(
    raw: &DriverTerminal,
    plan: &DeleteShareGroupOffsetsPlan,
    retained_limit: usize,
) -> (DeleteShareGroupOffsetsInput, usize) {
    match raw.fact() {
        DeleteShareGroupOffsetsTerminalFact::Response {
            selected_version,
            response,
        } => match normalize_delete_share_group_offsets_response(
            plan,
            selected_version,
            response,
            retained_limit,
        ) {
            Ok(normalized) => {
                let (result, retained_bytes) = normalized.into_parts();
                let input = match result {
                    Ok(batch) => DeleteShareGroupOffsetsInput::BrokerResponded { batch },
                    Err(error) => DeleteShareGroupOffsetsInput::BrokerRejected { error },
                };
                (input, retained_bytes)
            }
            Err(error) => (protocol_failure(error), 0),
        },
        DeleteShareGroupOffsetsTerminalFact::Failed { kind, delivery } => {
            (driver_failure(kind, delivery), 0)
        }
    }
}

pub(super) const fn protocol_failure(
    error: DeleteShareGroupOffsetsProtocolFailure,
) -> DeleteShareGroupOffsetsInput {
    match error {
        DeleteShareGroupOffsetsProtocolFailure::MissingSelectedVersion
        | DeleteShareGroupOffsetsProtocolFailure::UnsupportedApiVersion { .. } => {
            DeleteShareGroupOffsetsInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            }
        }
        DeleteShareGroupOffsetsProtocolFailure::NormalizedBytesExceeded { .. }
        | DeleteShareGroupOffsetsProtocolFailure::RetainedBytes { .. }
        | DeleteShareGroupOffsetsProtocolFailure::Allocation { .. }
        | DeleteShareGroupOffsetsProtocolFailure::TooManyTopics { .. }
        | DeleteShareGroupOffsetsProtocolFailure::TopicNameTooLong { .. }
        | DeleteShareGroupOffsetsProtocolFailure::ResponseTextBytesExceeded { .. } => {
            DeleteShareGroupOffsetsInput::ResponseTooLarge
        }
        DeleteShareGroupOffsetsProtocolFailure::NegativeThrottleTime { .. }
        | DeleteShareGroupOffsetsProtocolFailure::TopicCount { .. }
        | DeleteShareGroupOffsetsProtocolFailure::EmptyTopicName
        | DeleteShareGroupOffsetsProtocolFailure::DuplicateTopic
        | DeleteShareGroupOffsetsProtocolFailure::MissingTopic
        | DeleteShareGroupOffsetsProtocolFailure::UnexpectedTopic
        | DeleteShareGroupOffsetsProtocolFailure::DiagnosticOnSuccess
        | DeleteShareGroupOffsetsProtocolFailure::ZeroTopicId
        | DeleteShareGroupOffsetsProtocolFailure::TopicsOnTopLevelError => {
            DeleteShareGroupOffsetsInput::InvalidResponse
        }
    }
}

const fn driver_failure(
    kind: DeleteShareGroupOffsetsDriverFailureKind,
    delivery: DeliveryStatus,
) -> DeleteShareGroupOffsetsInput {
    match kind {
        DeleteShareGroupOffsetsDriverFailureKind::DeadlineElapsed => {
            DeleteShareGroupOffsetsInput::DriverDeadlineElapsed { delivery }
        }
        DeleteShareGroupOffsetsDriverFailureKind::Compatibility => {
            DeleteShareGroupOffsetsInput::ProtocolIncompatible { delivery }
        }
        DeleteShareGroupOffsetsDriverFailureKind::InvalidResponse => {
            DeleteShareGroupOffsetsInput::InvalidResponse
        }
        DeleteShareGroupOffsetsDriverFailureKind::Transport => {
            DeleteShareGroupOffsetsInput::TransportFailed { delivery }
        }
    }
}
