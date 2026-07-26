//! Forbidden generated OffsetFetch ownership outside the protocol and driver seams.

use kafka_wire::{
    OffsetFetchRequest, OffsetFetchResponse,
    offset_fetch_request::{
        OffsetFetchRequestGroup, OffsetFetchRequestTopic, OffsetFetchRequestTopics,
    },
    offset_fetch_response::{
        OffsetFetchResponseGroup, OffsetFetchResponsePartition, OffsetFetchResponsePartitions,
        OffsetFetchResponseTopic, OffsetFetchResponseTopics,
    },
};

fn retain_generated(
    _request: OffsetFetchRequest,
    _request_group: OffsetFetchRequestGroup,
    _request_topic: OffsetFetchRequestTopic,
    _request_topics: OffsetFetchRequestTopics,
    _response: OffsetFetchResponse,
    _response_group: OffsetFetchResponseGroup,
    _response_partition: OffsetFetchResponsePartition,
    _response_partitions: OffsetFetchResponsePartitions,
    _response_topic: OffsetFetchResponseTopic,
    _response_topics: OffsetFetchResponseTopics,
) {
}
