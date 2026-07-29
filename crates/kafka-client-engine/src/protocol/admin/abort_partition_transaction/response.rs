//! Strict one-marker `WriteTxnMarkers` response correlation.

use core::num::NonZeroI16;

use kafka_client_core::AbortPartitionTransactionPlan;
use kafka_wire::{
    WriteTxnMarkersResponse,
    write_txn_markers_response::{
        WritableTxnMarkerPartitionResult, WritableTxnMarkerResult, WritableTxnMarkerTopicResult,
    },
};

/// Structural response facts unsafe to bind to the admitted operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AbortPartitionTransactionResponseFailure {
    /// The selected version lies outside this slice's exact v1-v2 window.
    UnsupportedApiVersion {
        /// Exact selected Kafka API version.
        actual: i16,
    },
    /// The expected producer result was absent.
    MissingProducer,
    /// The expected producer result appeared more than once.
    DuplicateProducer,
    /// A result named a different producer.
    UnexpectedProducer {
        /// Exact unexpected producer identity.
        actual: i64,
    },
    /// The expected topic result was absent.
    MissingTopic,
    /// The expected topic result appeared more than once.
    DuplicateTopic,
    /// A result named a different topic.
    UnexpectedTopic,
    /// The expected partition result was absent.
    MissingPartition,
    /// The expected partition result appeared more than once.
    DuplicatePartition,
    /// A response partition used a negative index.
    InvalidPartition {
        /// Exact invalid partition index.
        actual: i32,
    },
    /// A result named a different nonnegative partition.
    UnexpectedPartition {
        /// Exact unexpected partition index.
        actual: i32,
    },
}

/// Correlates the complete response and returns Kafka's exact partition error.
pub(crate) fn normalize_abort_partition_transaction_response(
    plan: &AbortPartitionTransactionPlan,
    selected_version: i16,
    response: &WriteTxnMarkersResponse,
) -> Result<Option<NonZeroI16>, AbortPartitionTransactionResponseFailure> {
    if !(1..=2).contains(&selected_version) {
        return Err(
            AbortPartitionTransactionResponseFailure::UnsupportedApiVersion {
                actual: selected_version,
            },
        );
    }
    let marker = matching_producer(plan.producer_id(), &response.markers)?;
    let topic = matching_topic(plan.topic(), &marker.topics)?;
    let partition = matching_partition(plan.partition(), &topic.partitions)?;
    Ok(NonZeroI16::new(partition.error_code))
}

fn matching_producer(
    expected: i64,
    markers: &[WritableTxnMarkerResult],
) -> Result<&WritableTxnMarkerResult, AbortPartitionTransactionResponseFailure> {
    let mut matching = None;
    for marker in markers {
        if marker.producer_id != expected {
            return Err(
                AbortPartitionTransactionResponseFailure::UnexpectedProducer {
                    actual: marker.producer_id,
                },
            );
        }
        if matching.replace(marker).is_some() {
            return Err(AbortPartitionTransactionResponseFailure::DuplicateProducer);
        }
    }
    matching.ok_or(AbortPartitionTransactionResponseFailure::MissingProducer)
}

fn matching_topic<'a>(
    expected: &str,
    topics: &'a [WritableTxnMarkerTopicResult],
) -> Result<&'a WritableTxnMarkerTopicResult, AbortPartitionTransactionResponseFailure> {
    let mut matching = None;
    for topic in topics {
        if topic.name.as_str() != expected {
            return Err(AbortPartitionTransactionResponseFailure::UnexpectedTopic);
        }
        if matching.replace(topic).is_some() {
            return Err(AbortPartitionTransactionResponseFailure::DuplicateTopic);
        }
    }
    matching.ok_or(AbortPartitionTransactionResponseFailure::MissingTopic)
}

fn matching_partition(
    expected: i32,
    partitions: &[WritableTxnMarkerPartitionResult],
) -> Result<&WritableTxnMarkerPartitionResult, AbortPartitionTransactionResponseFailure> {
    let mut matching = None;
    for partition in partitions {
        if partition.partition_index < 0 {
            return Err(AbortPartitionTransactionResponseFailure::InvalidPartition {
                actual: partition.partition_index,
            });
        }
        if partition.partition_index != expected {
            return Err(
                AbortPartitionTransactionResponseFailure::UnexpectedPartition {
                    actual: partition.partition_index,
                },
            );
        }
        if matching.replace(partition).is_some() {
            return Err(AbortPartitionTransactionResponseFailure::DuplicatePartition);
        }
    }
    matching.ok_or(AbortPartitionTransactionResponseFailure::MissingPartition)
}
