//! Exact one-topic, one-partition `DescribeProducers` request construction.

use kafka_client_core::AdminDescribeProducerTarget;
use kafka_wire::{DescribeProducersRequest, describe_producers_request::TopicRequest};

/// Builds the sole generated v0 request for one already-validated target.
pub(crate) fn describe_producers_request(
    target: &AdminDescribeProducerTarget,
) -> DescribeProducersRequest {
    let mut topic = TopicRequest::default();
    topic.name = target.topic().into();
    topic.partition_indexes = vec![target.partition()];

    let mut request = DescribeProducersRequest::default();
    request.topics = vec![topic];
    request
}
