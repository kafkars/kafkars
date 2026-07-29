//! Exact single-target response shape correlation before scalar interpretation.

use kafka_client_core::AdminDescribeProducerTarget;
use kafka_wire::{DescribeProducersResponse, describe_producers_response::PartitionResponse};

use super::response::DescribeProducersProtocolFailure;

pub(super) fn correlated_partition<'a>(
    target: &AdminDescribeProducerTarget,
    response: &'a DescribeProducersResponse,
) -> Result<&'a PartitionResponse, DescribeProducersProtocolFailure> {
    let [topic] = response.topics.as_slice() else {
        return Err(DescribeProducersProtocolFailure::UnexpectedTopicCount {
            actual: response.topics.len(),
        });
    };
    if topic.name.as_str() != target.topic() {
        return Err(DescribeProducersProtocolFailure::UnexpectedTopic);
    }
    let [partition] = topic.partitions.as_slice() else {
        return Err(DescribeProducersProtocolFailure::UnexpectedPartitionCount {
            actual: topic.partitions.len(),
        });
    };
    if partition.partition_index < 0 {
        return Err(DescribeProducersProtocolFailure::NegativePartition {
            actual: partition.partition_index,
        });
    }
    if partition.partition_index != target.partition() {
        return Err(DescribeProducersProtocolFailure::UnexpectedPartition {
            actual: partition.partition_index,
        });
    }
    Ok(partition)
}
