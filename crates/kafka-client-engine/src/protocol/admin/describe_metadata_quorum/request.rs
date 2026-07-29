//! Exact fixed metadata-quorum request construction across generated v0-v2.

use kafka_wire::{
    DescribeQuorumRequest,
    describe_quorum_request::{PartitionData, TopicData},
};

pub(super) const METADATA_TOPIC: &str = "__cluster_metadata";
pub(super) const METADATA_PARTITION: i32 = 0;

/// Builds the sole metadata-quorum selection without adding routing policy.
pub(crate) fn describe_metadata_quorum_request() -> DescribeQuorumRequest {
    let mut partition = PartitionData::default();
    partition.partition_index = METADATA_PARTITION;

    let mut topic = TopicData::default();
    topic.topic_name = METADATA_TOPIC.into();
    topic.partitions = vec![partition];

    let mut request = DescribeQuorumRequest::default();
    request.topics = vec![topic];
    request
}
