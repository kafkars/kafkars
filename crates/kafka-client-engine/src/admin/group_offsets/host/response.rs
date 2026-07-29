//! Exhaustive singleton-response translation for singular and batched operations.

use kafka_client_core::{
    GroupOffsetBrokerError, GroupOffsetDescription, GroupOffsetOutcome,
    ListConsumerGroupOffsetsBatch, ListConsumerGroupOffsetsInput,
};

use crate::{
    driver::{GroupOffsetsDriverFailureKind, GroupOffsetsTerminal, GroupOffsetsTerminalFact},
    protocol::admin::group_offsets::{
        GroupOffsetValueRef, GroupOffsetsProtocolFailure, ValidatedGroupOffsetsResponse,
        validate_group_offsets_response,
    },
};

pub(super) fn terminal_input(
    terminal: &GroupOffsetsTerminal,
    group_id: &str,
    result_limit: usize,
) -> (ListConsumerGroupOffsetsInput, usize) {
    match terminal.fact() {
        GroupOffsetsTerminalFact::Failed { kind, delivery } => match kind {
            GroupOffsetsDriverFailureKind::DeadlineElapsed => (
                ListConsumerGroupOffsetsInput::DriverDeadlineElapsed { delivery },
                0,
            ),
            GroupOffsetsDriverFailureKind::Compatibility => (
                ListConsumerGroupOffsetsInput::ProtocolIncompatible { delivery },
                0,
            ),
            GroupOffsetsDriverFailureKind::InvalidResponse => {
                (ListConsumerGroupOffsetsInput::InvalidResponse, 0)
            }
            GroupOffsetsDriverFailureKind::Transport => (
                ListConsumerGroupOffsetsInput::TransportFailed { delivery },
                0,
            ),
        },
        GroupOffsetsTerminalFact::Response {
            selected_version: None,
            ..
        } => (ListConsumerGroupOffsetsInput::InvalidResponse, 0),
        GroupOffsetsTerminalFact::Response {
            selected_version: Some(selected_version),
            response,
        } => match validate_group_offsets_response(
            group_id,
            response,
            selected_version,
            result_limit,
        ) {
            Ok(validated) => {
                let retained_charge = validated.retained_charge();
                (normalized_input(validated), retained_charge)
            }
            Err(error) => (protocol_failure(error), 0),
        },
    }
}

fn normalized_input(validated: ValidatedGroupOffsetsResponse<'_>) -> ListConsumerGroupOffsetsInput {
    if let Some(code) = validated.top_level_error() {
        return ListConsumerGroupOffsetsInput::BrokerRejected {
            code,
            throttle_time_ms: validated.throttle_time_ms(),
        };
    }
    let throttle_time_ms = validated.throttle_time_ms();
    let outcomes = validated
        .into_validated_offsets()
        .into_iter()
        .map(|entry| match entry.value() {
            GroupOffsetValueRef::Committed {
                offset,
                leader_epoch,
                metadata,
            } => GroupOffsetOutcome::described(
                entry.topic().to_owned(),
                entry.partition(),
                GroupOffsetDescription::new(offset, leader_epoch, metadata.map(str::to_owned)),
            ),
            GroupOffsetValueRef::Rejected { code } => GroupOffsetOutcome::failed(
                entry.topic().to_owned(),
                entry.partition(),
                GroupOffsetBrokerError::new(code),
            ),
        })
        .collect();
    ListConsumerGroupOffsetsInput::BrokerResponded {
        batch: ListConsumerGroupOffsetsBatch::new(throttle_time_ms, outcomes),
    }
}

const fn protocol_failure(error: GroupOffsetsProtocolFailure) -> ListConsumerGroupOffsetsInput {
    match error {
        GroupOffsetsProtocolFailure::UnsupportedApiVersion { .. } => {
            ListConsumerGroupOffsetsInput::ProtocolIncompatible {
                delivery: kafka_client_core::DeliveryStatus::PossiblySent,
            }
        }
        GroupOffsetsProtocolFailure::RetainedBytes => {
            ListConsumerGroupOffsetsInput::ResponseTooLarge
        }
        GroupOffsetsProtocolFailure::NegativeThrottleTime { .. }
        | GroupOffsetsProtocolFailure::UnexpectedLegacyResults
        | GroupOffsetsProtocolFailure::UnexpectedMultiGroupResults
        | GroupOffsetsProtocolFailure::MissingGroup
        | GroupOffsetsProtocolFailure::UnexpectedGroup
        | GroupOffsetsProtocolFailure::DuplicateGroup
        | GroupOffsetsProtocolFailure::EmptyTopic
        | GroupOffsetsProtocolFailure::EmptyTopicPartitions
        | GroupOffsetsProtocolFailure::DuplicateTopic
        | GroupOffsetsProtocolFailure::NegativePartition { .. }
        | GroupOffsetsProtocolFailure::DuplicatePartition { .. }
        | GroupOffsetsProtocolFailure::InvalidCommittedOffset { .. }
        | GroupOffsetsProtocolFailure::InvalidLeaderEpoch { .. } => {
            ListConsumerGroupOffsetsInput::InvalidResponse
        }
    }
}
