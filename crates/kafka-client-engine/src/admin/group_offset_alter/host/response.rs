//! Exhaustive translation from borrowed `OffsetCommit` facts into core input.

use kafka_client_core::{
    AlterConsumerGroupOffsetBrokerError, AlterConsumerGroupOffsetOutcome,
    AlterConsumerGroupOffsetsBatch, AlterConsumerGroupOffsetsInput, AlterConsumerGroupOffsetsPlan,
};

use crate::{
    driver::{
        GroupOffsetAlterDriverFailureKind, GroupOffsetAlterTerminal, GroupOffsetAlterTerminalFact,
    },
    protocol::admin::group_offset_alter::{
        GroupOffsetAlterProtocolFailure, OffsetCommitPartitionResult, OffsetCommitTargetRef,
        ValidatedOffsetCommitResponse, validate_group_offset_alter_response,
    },
};

pub(super) fn terminal_input(
    terminal: &GroupOffsetAlterTerminal,
) -> AlterConsumerGroupOffsetsInput {
    match terminal.fact() {
        GroupOffsetAlterTerminalFact::Failed { kind, delivery } => match kind {
            GroupOffsetAlterDriverFailureKind::DeadlineElapsed => {
                AlterConsumerGroupOffsetsInput::DriverDeadlineElapsed { delivery }
            }
            GroupOffsetAlterDriverFailureKind::Compatibility => {
                AlterConsumerGroupOffsetsInput::ProtocolIncompatible { delivery }
            }
            GroupOffsetAlterDriverFailureKind::InvalidResponse => {
                AlterConsumerGroupOffsetsInput::InvalidResponse
            }
            GroupOffsetAlterDriverFailureKind::Transport => {
                AlterConsumerGroupOffsetsInput::TransportFailed { delivery }
            }
        },
        GroupOffsetAlterTerminalFact::Response {
            selected_version: None,
            ..
        } => AlterConsumerGroupOffsetsInput::InvalidResponse,
        GroupOffsetAlterTerminalFact::Response {
            selected_version: Some(selected_version),
            response,
        } => {
            let targets = match target_refs(terminal.response_plan()) {
                Ok(targets) => targets,
                Err(input) => return input,
            };
            match validate_group_offset_alter_response(
                &targets,
                response,
                selected_version,
                terminal.result_limit(),
            ) {
                Ok(validated) => normalized_input(validated),
                Err(error) => protocol_failure(error),
            }
        }
    }
}

fn target_refs(
    plan: &AlterConsumerGroupOffsetsPlan,
) -> Result<Vec<OffsetCommitTargetRef<'_>>, AlterConsumerGroupOffsetsInput> {
    let mut targets = Vec::new();
    targets
        .try_reserve_exact(plan.targets().len())
        .map_err(|_error| AlterConsumerGroupOffsetsInput::ResponseTooLarge)?;
    targets.extend(plan.targets().iter().map(|target| {
        OffsetCommitTargetRef::new(
            target.topic(),
            target.partition(),
            target.next_offset(),
            target.leader_epoch(),
            target.metadata(),
        )
    }));
    Ok(targets)
}

fn normalized_input(
    validated: ValidatedOffsetCommitResponse<'_>,
) -> AlterConsumerGroupOffsetsInput {
    let throttle_time_ms = validated.throttle_time_ms();
    let outcomes = validated
        .into_validated_alterations()
        .into_iter()
        .map(|entry| match entry.result() {
            OffsetCommitPartitionResult::Altered => AlterConsumerGroupOffsetOutcome::altered(
                entry.topic().to_owned(),
                entry.partition(),
            ),
            OffsetCommitPartitionResult::Rejected { code } => {
                AlterConsumerGroupOffsetOutcome::failed(
                    entry.topic().to_owned(),
                    entry.partition(),
                    AlterConsumerGroupOffsetBrokerError::new(code),
                )
            }
        })
        .collect();
    AlterConsumerGroupOffsetsInput::BrokerResponded {
        batch: AlterConsumerGroupOffsetsBatch::new(throttle_time_ms, outcomes),
    }
}

const fn protocol_failure(
    error: GroupOffsetAlterProtocolFailure,
) -> AlterConsumerGroupOffsetsInput {
    match error {
        GroupOffsetAlterProtocolFailure::UnsupportedApiVersion { .. } => {
            AlterConsumerGroupOffsetsInput::ProtocolIncompatible {
                delivery: kafka_client_core::DeliveryStatus::PossiblySent,
            }
        }
        GroupOffsetAlterProtocolFailure::RetainedBytes => {
            AlterConsumerGroupOffsetsInput::ResponseTooLarge
        }
        GroupOffsetAlterProtocolFailure::NegativeThrottleTime { .. }
        | GroupOffsetAlterProtocolFailure::TopicCount { .. }
        | GroupOffsetAlterProtocolFailure::UnexpectedTopic
        | GroupOffsetAlterProtocolFailure::MissingTopic
        | GroupOffsetAlterProtocolFailure::DuplicateTopic
        | GroupOffsetAlterProtocolFailure::EmptyTopic
        | GroupOffsetAlterProtocolFailure::EmptyTopicPartitions
        | GroupOffsetAlterProtocolFailure::PartitionCount { .. }
        | GroupOffsetAlterProtocolFailure::UnexpectedPartition { .. }
        | GroupOffsetAlterProtocolFailure::MissingPartition { .. }
        | GroupOffsetAlterProtocolFailure::DuplicatePartition { .. }
        | GroupOffsetAlterProtocolFailure::DuplicateTarget { .. }
        | GroupOffsetAlterProtocolFailure::NegativePartition { .. } => {
            AlterConsumerGroupOffsetsInput::InvalidResponse
        }
    }
}
