//! Exhaustive translation from borrowed `OffsetDelete` facts into core input.

use kafka_client_core::{
    DeleteConsumerGroupOffsetBrokerError, DeleteConsumerGroupOffsetOutcome,
    DeleteConsumerGroupOffsetsBatch, DeleteConsumerGroupOffsetsInput,
    DeleteConsumerGroupOffsetsPlan,
};

use crate::{
    driver::{
        GroupOffsetDeleteDriverFailureKind, GroupOffsetDeleteTerminal,
        GroupOffsetDeleteTerminalFact,
    },
    protocol::admin::group_offset_delete::{
        GroupOffsetDeleteProtocolFailure, OffsetDeletePartitionResult, OffsetDeleteTargetRef,
        ValidatedOffsetDeleteResponse, validate_group_offset_delete_response,
    },
};

pub(super) fn terminal_input(
    terminal: &GroupOffsetDeleteTerminal,
    plan: &DeleteConsumerGroupOffsetsPlan,
    result_limit: usize,
) -> DeleteConsumerGroupOffsetsInput {
    match terminal.fact() {
        GroupOffsetDeleteTerminalFact::Failed { kind, delivery } => match kind {
            GroupOffsetDeleteDriverFailureKind::DeadlineElapsed => {
                DeleteConsumerGroupOffsetsInput::DriverDeadlineElapsed { delivery }
            }
            GroupOffsetDeleteDriverFailureKind::Compatibility => {
                DeleteConsumerGroupOffsetsInput::ProtocolIncompatible { delivery }
            }
            GroupOffsetDeleteDriverFailureKind::InvalidResponse => {
                DeleteConsumerGroupOffsetsInput::InvalidResponse
            }
            GroupOffsetDeleteDriverFailureKind::Transport => {
                DeleteConsumerGroupOffsetsInput::TransportFailed { delivery }
            }
        },
        GroupOffsetDeleteTerminalFact::Response {
            selected_version: None,
            ..
        } => DeleteConsumerGroupOffsetsInput::InvalidResponse,
        GroupOffsetDeleteTerminalFact::Response {
            selected_version: Some(selected_version),
            response,
        } => {
            let targets = match target_refs(plan) {
                Ok(targets) => targets,
                Err(input) => return input,
            };
            match validate_group_offset_delete_response(
                &targets,
                response,
                selected_version,
                result_limit,
            ) {
                Ok(validated) => normalized_input(validated),
                Err(error) => protocol_failure(error),
            }
        }
    }
}

fn target_refs(
    plan: &DeleteConsumerGroupOffsetsPlan,
) -> Result<Vec<OffsetDeleteTargetRef<'_>>, DeleteConsumerGroupOffsetsInput> {
    let mut targets = Vec::new();
    targets
        .try_reserve_exact(plan.targets().len())
        .map_err(|_error| DeleteConsumerGroupOffsetsInput::ResponseTooLarge)?;
    targets.extend(
        plan.targets()
            .iter()
            .map(|target| OffsetDeleteTargetRef::new(target.topic(), target.partition())),
    );
    Ok(targets)
}

fn normalized_input(
    validated: ValidatedOffsetDeleteResponse<'_>,
) -> DeleteConsumerGroupOffsetsInput {
    if let Some(code) = validated.top_level_error() {
        return DeleteConsumerGroupOffsetsInput::BrokerRejected { code };
    }
    let throttle_time_ms = validated.throttle_time_ms();
    let outcomes = validated
        .into_validated_deletions()
        .into_iter()
        .map(|entry| match entry.result() {
            OffsetDeletePartitionResult::Deleted => DeleteConsumerGroupOffsetOutcome::deleted(
                entry.topic().to_owned(),
                entry.partition(),
            ),
            OffsetDeletePartitionResult::Rejected { code } => {
                DeleteConsumerGroupOffsetOutcome::failed(
                    entry.topic().to_owned(),
                    entry.partition(),
                    DeleteConsumerGroupOffsetBrokerError::new(code),
                )
            }
        })
        .collect();
    DeleteConsumerGroupOffsetsInput::BrokerResponded {
        batch: DeleteConsumerGroupOffsetsBatch::new(throttle_time_ms, outcomes),
    }
}

const fn protocol_failure(
    error: GroupOffsetDeleteProtocolFailure,
) -> DeleteConsumerGroupOffsetsInput {
    match error {
        GroupOffsetDeleteProtocolFailure::UnsupportedApiVersion { .. } => {
            DeleteConsumerGroupOffsetsInput::ProtocolIncompatible {
                delivery: kafka_client_core::DeliveryStatus::PossiblySent,
            }
        }
        GroupOffsetDeleteProtocolFailure::RetainedBytes => {
            DeleteConsumerGroupOffsetsInput::ResponseTooLarge
        }
        GroupOffsetDeleteProtocolFailure::NegativeThrottleTime { .. }
        | GroupOffsetDeleteProtocolFailure::TopicCount { .. }
        | GroupOffsetDeleteProtocolFailure::UnexpectedTopic
        | GroupOffsetDeleteProtocolFailure::MissingTopic
        | GroupOffsetDeleteProtocolFailure::DuplicateTopic
        | GroupOffsetDeleteProtocolFailure::EmptyTopic
        | GroupOffsetDeleteProtocolFailure::EmptyTopicPartitions
        | GroupOffsetDeleteProtocolFailure::PartitionCount { .. }
        | GroupOffsetDeleteProtocolFailure::UnexpectedPartition { .. }
        | GroupOffsetDeleteProtocolFailure::MissingPartition { .. }
        | GroupOffsetDeleteProtocolFailure::DuplicatePartition { .. }
        | GroupOffsetDeleteProtocolFailure::DuplicateTarget { .. }
        | GroupOffsetDeleteProtocolFailure::NegativePartition { .. } => {
            DeleteConsumerGroupOffsetsInput::InvalidResponse
        }
    }
}
