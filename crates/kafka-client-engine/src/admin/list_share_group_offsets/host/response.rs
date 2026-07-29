//! Exhaustive generated-free response and driver-failure translation.

use kafka_client_core::{DeliveryStatus, ListShareGroupOffsetsInput, ListShareGroupOffsetsPlan};

use crate::{
    driver::{
        ListShareGroupOffsetsDriverFailureKind, ListShareGroupOffsetsTerminal as DriverTerminal,
        ListShareGroupOffsetsTerminalFact,
    },
    protocol::admin::list_share_group_offsets::{
        ListShareGroupOffsetsProtocolFailure, normalize_list_share_group_offsets_response,
    },
};

pub(super) fn terminal_input(
    raw: &DriverTerminal,
    plan: &ListShareGroupOffsetsPlan,
    retained_limit: usize,
) -> (ListShareGroupOffsetsInput, usize) {
    match raw.fact() {
        ListShareGroupOffsetsTerminalFact::Response {
            selected_version,
            response,
        } => match normalize_list_share_group_offsets_response(
            plan,
            selected_version,
            response,
            retained_limit,
        ) {
            Ok(normalized) => {
                let (result, retained_bytes) = normalized.into_parts();
                let input = match result {
                    Ok(batch) => ListShareGroupOffsetsInput::BrokerResponded { batch },
                    Err(error) => ListShareGroupOffsetsInput::BrokerRejected { error },
                };
                (input, retained_bytes)
            }
            Err(error) => (protocol_failure(error), 0),
        },
        ListShareGroupOffsetsTerminalFact::Failed { kind, delivery } => {
            (driver_failure(kind, delivery), 0)
        }
    }
}

pub(super) const fn protocol_failure(
    error: ListShareGroupOffsetsProtocolFailure,
) -> ListShareGroupOffsetsInput {
    match error {
        ListShareGroupOffsetsProtocolFailure::MissingSelectedVersion
        | ListShareGroupOffsetsProtocolFailure::UnsupportedApiVersion { .. } => {
            ListShareGroupOffsetsInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            }
        }
        ListShareGroupOffsetsProtocolFailure::NormalizedBytesExceeded { .. }
        | ListShareGroupOffsetsProtocolFailure::RetainedBytes { .. }
        | ListShareGroupOffsetsProtocolFailure::Allocation { .. }
        | ListShareGroupOffsetsProtocolFailure::TooManyTopics { .. }
        | ListShareGroupOffsetsProtocolFailure::TooManyPartitions { .. }
        | ListShareGroupOffsetsProtocolFailure::TopicNameTooLong { .. }
        | ListShareGroupOffsetsProtocolFailure::ResponseTextBytesExceeded { .. } => {
            ListShareGroupOffsetsInput::ResponseTooLarge
        }
        ListShareGroupOffsetsProtocolFailure::NegativeThrottleTime { .. }
        | ListShareGroupOffsetsProtocolFailure::GroupCount { .. }
        | ListShareGroupOffsetsProtocolFailure::UnexpectedGroup
        | ListShareGroupOffsetsProtocolFailure::EmptyTopicName
        | ListShareGroupOffsetsProtocolFailure::EmptyTopicPartitions
        | ListShareGroupOffsetsProtocolFailure::DuplicateTopic
        | ListShareGroupOffsetsProtocolFailure::NegativePartition { .. }
        | ListShareGroupOffsetsProtocolFailure::DuplicatePartition { .. }
        | ListShareGroupOffsetsProtocolFailure::MissingPartition
        | ListShareGroupOffsetsProtocolFailure::UnexpectedPartition
        | ListShareGroupOffsetsProtocolFailure::DiagnosticOnSuccess
        | ListShareGroupOffsetsProtocolFailure::ZeroTopicId
        | ListShareGroupOffsetsProtocolFailure::InvalidStartOffset { .. }
        | ListShareGroupOffsetsProtocolFailure::InvalidLeaderEpoch { .. }
        | ListShareGroupOffsetsProtocolFailure::InvalidV0Lag { .. }
        | ListShareGroupOffsetsProtocolFailure::InvalidLag { .. } => {
            ListShareGroupOffsetsInput::InvalidResponse
        }
    }
}

const fn driver_failure(
    kind: ListShareGroupOffsetsDriverFailureKind,
    delivery: DeliveryStatus,
) -> ListShareGroupOffsetsInput {
    match kind {
        ListShareGroupOffsetsDriverFailureKind::DeadlineElapsed => {
            ListShareGroupOffsetsInput::DriverDeadlineElapsed { delivery }
        }
        ListShareGroupOffsetsDriverFailureKind::Compatibility => {
            ListShareGroupOffsetsInput::ProtocolIncompatible { delivery }
        }
        ListShareGroupOffsetsDriverFailureKind::InvalidResponse => {
            ListShareGroupOffsetsInput::InvalidResponse
        }
        ListShareGroupOffsetsDriverFailureKind::Transport => {
            ListShareGroupOffsetsInput::TransportFailed { delivery }
        }
    }
}
