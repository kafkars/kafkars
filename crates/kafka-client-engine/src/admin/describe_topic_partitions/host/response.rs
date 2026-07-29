//! Exhaustive generated-free response and driver-failure translation.

use kafka_client_core::{
    DeliveryStatus, DescribeTopicPartition, DescribeTopicPartitionsCursor,
    DescribeTopicPartitionsInput, DescribeTopicPartitionsPage, DescribeTopicPartitionsTopic,
    DescribeTopicPartitionsValueError,
};

use crate::{
    driver::{
        DescribeTopicPartitionsDriverFailureKind, DescribeTopicPartitionsRawTerminal,
        DescribeTopicPartitionsTerminalFact,
    },
    protocol::admin::describe_topic_partitions::{
        DescribeTopicPartitionsProtocolFailure, NormalizedDescribeTopicPartition,
        NormalizedDescribeTopicPartitionsResponse, NormalizedDescribeTopicPartitionsTopic,
        normalize_describe_topic_partitions_response,
    },
};

pub(super) fn terminal_input(
    raw: &DescribeTopicPartitionsRawTerminal,
    retained_limit: usize,
) -> (DescribeTopicPartitionsInput, usize) {
    match raw.fact() {
        DescribeTopicPartitionsTerminalFact::Response {
            selected_version: Some(selected_version),
            response,
        } => match normalize_describe_topic_partitions_response(
            selected_version,
            response,
            retained_limit,
        ) {
            Ok(normalized) => normalized_input(normalized),
            Err(error) => (protocol_failure(error), 0),
        },
        DescribeTopicPartitionsTerminalFact::Response {
            selected_version: None,
            ..
        } => (
            DescribeTopicPartitionsInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            },
            0,
        ),
        DescribeTopicPartitionsTerminalFact::Failed { kind, delivery } => {
            (driver_failure(kind, delivery), 0)
        }
    }
}

pub(super) fn normalized_input(
    normalized: NormalizedDescribeTopicPartitionsResponse,
) -> (DescribeTopicPartitionsInput, usize) {
    let (throttle_time_ms, topics, next_cursor, retained_bytes) = normalized.into_parts();
    let page = core_page(throttle_time_ms, topics, next_cursor);
    match page {
        Ok(page) => (
            DescribeTopicPartitionsInput::BrokerResponded { page },
            retained_bytes,
        ),
        Err(CorePageFailure::ResponseTooLarge) => {
            (DescribeTopicPartitionsInput::ResponseTooLarge, 0)
        }
        Err(CorePageFailure::InvalidResponse) => (DescribeTopicPartitionsInput::InvalidResponse, 0),
    }
}

fn core_page(
    throttle_time_ms: u32,
    topics: Vec<NormalizedDescribeTopicPartitionsTopic>,
    next_cursor: Option<
        crate::protocol::admin::describe_topic_partitions::NormalizedDescribeTopicPartitionsCursor,
    >,
) -> Result<DescribeTopicPartitionsPage, CorePageFailure> {
    let topics = topics
        .into_iter()
        .map(core_topic)
        .collect::<Result<Vec<_>, _>>()?;
    let next_cursor = next_cursor
        .map(|cursor| {
            let (topic_name, partition_index) = cursor.into_parts();
            DescribeTopicPartitionsCursor::new(topic_name, partition_index)
                .map_err(|_| CorePageFailure::InvalidResponse)
        })
        .transpose()?;
    DescribeTopicPartitionsPage::new(throttle_time_ms, topics, next_cursor).map_err(value_failure)
}

fn core_topic(
    topic: NormalizedDescribeTopicPartitionsTopic,
) -> Result<DescribeTopicPartitionsTopic, CorePageFailure> {
    let (error_code, name, topic_id, internal, partitions, authorized_operations) =
        topic.into_parts();
    let name = name.ok_or(CorePageFailure::InvalidResponse)?;
    let partitions = partitions
        .into_iter()
        .map(core_partition)
        .collect::<Result<Vec<_>, _>>()?;
    DescribeTopicPartitionsTopic::new(
        error_code,
        name,
        topic_id,
        internal,
        partitions,
        authorized_operations,
    )
    .map_err(value_failure)
}

fn core_partition(
    partition: NormalizedDescribeTopicPartition,
) -> Result<DescribeTopicPartition, CorePageFailure> {
    let (
        error_code,
        partition_index,
        leader_id,
        leader_epoch,
        replicas,
        isr,
        eligible_leader_replicas,
        last_known_elr,
        offline_replicas,
    ) = partition.into_parts();
    DescribeTopicPartition::new(
        error_code,
        partition_index,
        leader_id,
        leader_epoch,
        replicas,
        isr,
        eligible_leader_replicas,
        last_known_elr,
        offline_replicas,
    )
    .map_err(value_failure)
}

const fn value_failure(error: DescribeTopicPartitionsValueError) -> CorePageFailure {
    match error {
        DescribeTopicPartitionsValueError::RetainedBytesExceeded => {
            CorePageFailure::ResponseTooLarge
        }
        _ => CorePageFailure::InvalidResponse,
    }
}

pub(super) const fn protocol_failure(
    error: DescribeTopicPartitionsProtocolFailure,
) -> DescribeTopicPartitionsInput {
    match error {
        DescribeTopicPartitionsProtocolFailure::UnsupportedApiVersion { .. } => {
            DescribeTopicPartitionsInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            }
        }
        DescribeTopicPartitionsProtocolFailure::RetainedBytes { .. }
        | DescribeTopicPartitionsProtocolFailure::Allocation { .. } => {
            DescribeTopicPartitionsInput::ResponseTooLarge
        }
        DescribeTopicPartitionsProtocolFailure::NegativeThrottleTime { .. }
        | DescribeTopicPartitionsProtocolFailure::TooManyTopics { .. }
        | DescribeTopicPartitionsProtocolFailure::TooManyPartitions { .. }
        | DescribeTopicPartitionsProtocolFailure::TooManyBrokerReferences { .. }
        | DescribeTopicPartitionsProtocolFailure::ResponseTopicBytesExceeded { .. }
        | DescribeTopicPartitionsProtocolFailure::EmptyTopicName
        | DescribeTopicPartitionsProtocolFailure::TopicNameTooLong { .. }
        | DescribeTopicPartitionsProtocolFailure::DuplicateTopicName
        | DescribeTopicPartitionsProtocolFailure::NegativePartition { .. }
        | DescribeTopicPartitionsProtocolFailure::DuplicatePartition { .. }
        | DescribeTopicPartitionsProtocolFailure::InvalidLeaderId { .. }
        | DescribeTopicPartitionsProtocolFailure::InvalidLeaderEpoch { .. }
        | DescribeTopicPartitionsProtocolFailure::NegativeBrokerId { .. }
        | DescribeTopicPartitionsProtocolFailure::DuplicateBrokerId { .. }
        | DescribeTopicPartitionsProtocolFailure::EmptyCursorTopic
        | DescribeTopicPartitionsProtocolFailure::CursorTopicTooLong { .. }
        | DescribeTopicPartitionsProtocolFailure::NegativeCursorPartition { .. } => {
            DescribeTopicPartitionsInput::InvalidResponse
        }
    }
}

const fn driver_failure(
    kind: DescribeTopicPartitionsDriverFailureKind,
    delivery: DeliveryStatus,
) -> DescribeTopicPartitionsInput {
    match kind {
        DescribeTopicPartitionsDriverFailureKind::DeadlineElapsed => {
            DescribeTopicPartitionsInput::DriverDeadlineElapsed { delivery }
        }
        DescribeTopicPartitionsDriverFailureKind::Compatibility => {
            DescribeTopicPartitionsInput::ProtocolIncompatible { delivery }
        }
        DescribeTopicPartitionsDriverFailureKind::InvalidResponse => {
            DescribeTopicPartitionsInput::InvalidResponse
        }
        DescribeTopicPartitionsDriverFailureKind::Transport => {
            DescribeTopicPartitionsInput::TransportFailed { delivery }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CorePageFailure {
    ResponseTooLarge,
    InvalidResponse,
}
