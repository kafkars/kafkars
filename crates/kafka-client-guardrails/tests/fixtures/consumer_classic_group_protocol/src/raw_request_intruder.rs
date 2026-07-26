//! Deliberate raw generated request, response, and topology-view escape.

use kafka_driver::TopicView;
use kafka_wire::{
    HeartbeatRequest, JoinGroupRequest, MetadataRequest, MetadataResponse, SyncGroupRequest,
    metadata_request::MetadataRequestTopic,
    metadata_response::{MetadataResponsePartition, MetadataResponseTopic},
};

fn escape(
    heartbeat: HeartbeatRequest,
    join: JoinGroupRequest,
    metadata_request: MetadataRequest,
    metadata_request_topic: MetadataRequestTopic,
    metadata_response: MetadataResponse,
    metadata_response_partition: MetadataResponsePartition,
    metadata_response_topic: MetadataResponseTopic,
    sync: SyncGroupRequest,
    view: TopicView,
) {
    drop((
        heartbeat,
        join,
        metadata_request,
        metadata_request_topic,
        metadata_response,
        metadata_response_partition,
        metadata_response_topic,
        sync,
        view,
    ));
}
