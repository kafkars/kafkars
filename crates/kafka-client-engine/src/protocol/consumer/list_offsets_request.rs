//! Bounded one-partition `ListOffsets` request construction.

use kafka_client_core::{PartitionIndex, StartPosition};
use kafka_wire::{
    ListOffsetsRequest,
    list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic},
};

use super::ListOffsetsIsolation;

const CONSUMER_REPLICA_ID: i32 = -1;
const EARLIEST_TIMESTAMP: i64 = -2;
const LATEST_TIMESTAMP: i64 = -1;
const MAX_TOPIC_NAME_BYTES: usize = 249;

/// Why a position effect could not become a generated request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ListOffsetsRequestFailure {
    /// The engine catalog supplied an empty topic.
    EmptyTopic,
    /// The engine catalog supplied a topic outside Kafka's name bound.
    TopicTooLong {
        /// Actual UTF-8 byte count.
        actual: usize,
        /// Kafka topic-name byte limit.
        limit: usize,
    },
    /// The core partition cannot fit Kafka's signed partition field.
    PartitionOutOfRange {
        /// Exact zero-based partition supplied by core.
        actual: u32,
    },
    /// Position resolution is unnecessary for an explicit offset.
    ExplicitOffset,
    /// A remaining remote-storage timeout cannot be negative.
    NegativeTimeout {
        /// Exact invalid timeout supplied by the interpreter.
        actual: i32,
    },
}

/// Builds one normal-consumer query for one core-fenced position effect.
pub(crate) fn list_offsets_request(
    topic: &str,
    partition: PartitionIndex,
    position: StartPosition,
    isolation: ListOffsetsIsolation,
    timeout_ms: i32,
) -> Result<ListOffsetsRequest, ListOffsetsRequestFailure> {
    if topic.is_empty() {
        return Err(ListOffsetsRequestFailure::EmptyTopic);
    }
    if topic.len() > MAX_TOPIC_NAME_BYTES {
        return Err(ListOffsetsRequestFailure::TopicTooLong {
            actual: topic.len(),
            limit: MAX_TOPIC_NAME_BYTES,
        });
    }
    let partition_index = i32::try_from(partition.get()).map_err(|_| {
        ListOffsetsRequestFailure::PartitionOutOfRange {
            actual: partition.get(),
        }
    })?;
    let timestamp = match position {
        StartPosition::Beginning => EARLIEST_TIMESTAMP,
        StartPosition::End => LATEST_TIMESTAMP,
        StartPosition::Offset(_) => return Err(ListOffsetsRequestFailure::ExplicitOffset),
    };
    if timeout_ms < 0 {
        return Err(ListOffsetsRequestFailure::NegativeTimeout { actual: timeout_ms });
    }

    let mut generated_partition = ListOffsetsPartition::default();
    generated_partition.partition_index = partition_index;
    generated_partition.current_leader_epoch = -1;
    generated_partition.timestamp = timestamp;

    let mut generated_topic = ListOffsetsTopic::default();
    generated_topic.name = topic.into();
    generated_topic.partitions = vec![generated_partition];

    let mut request = ListOffsetsRequest::default();
    request.replica_id = CONSUMER_REPLICA_ID;
    request.isolation_level = isolation.wire_value();
    request.topics = vec![generated_topic];
    request.timeout_ms = timeout_ms;
    Ok(request)
}
