//! Generated discovery and API-key 2 responses for Admin `ListOffsets`.

use kafka_wire::{
    ApiVersionsRequest, ApiVersionsResponse, DescribeClusterRequest, DescribeClusterResponse,
    KafkaRequest, ListOffsetsRequest, ListOffsetsResponse, MetadataRequest, MetadataResponse,
    api_versions_response::ApiVersion as AdvertisedApi,
    describe_cluster_response::DescribeClusterBroker,
    list_offsets_response::{ListOffsetsPartitionResponse, ListOffsetsTopicResponse},
    metadata_response::{MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic},
};
use kafka_wire_core::StrBytes;

use super::{
    broker::Workflow,
    frame::{RequestFrame, encoded_response},
};

pub(super) const LIST_OFFSETS: i16 = 2;
pub(super) const METADATA: i16 = 3;
const API_VERSIONS: i16 = 18;
const DESCRIBE_CLUSTER: i16 = 60;

pub(super) fn for_request(
    request: &RequestFrame,
    workflow: Workflow,
    bootstrap_port: u16,
    leader_port: u16,
) -> Vec<u8> {
    match request.api_key {
        API_VERSIONS => api_versions(request, workflow),
        METADATA => metadata(request, bootstrap_port, leader_port),
        DESCRIBE_CLUSTER => describe_cluster(request, bootstrap_port, leader_port),
        LIST_OFFSETS => list_offsets(request),
        other => panic!("unexpected Admin ListOffsets Kafka API key {other}"),
    }
}

fn api_versions(request: &RequestFrame, workflow: Workflow) -> Vec<u8> {
    let _decoded: ApiVersionsRequest = request.decode();
    let maximum = match workflow {
        Workflow::Kafka43 => 11,
        Workflow::NoEarliestPendingUpload => 10,
    };
    let mut response = ApiVersionsResponse::default();
    response.api_keys = vec![
        advertised::<ApiVersionsRequest>(0, 0),
        advertised::<ListOffsetsRequest>(1, maximum),
        advertised::<MetadataRequest>(0, 1),
        advertised::<DescribeClusterRequest>(0, 2),
    ];
    encoded_response::<ApiVersionsRequest, _>(
        request.correlation_id,
        &response,
        request.api_version,
    )
}

fn metadata(request: &RequestFrame, bootstrap_port: u16, leader_port: u16) -> Vec<u8> {
    let decoded: MetadataRequest = request.decode();
    let include_orders = match decoded.topics.as_deref() {
        None => true,
        Some([]) => false,
        Some([topic]) => {
            assert_eq!(topic.name.as_ref().map(StrBytes::as_str), Some("orders"));
            true
        }
        Some(_) => panic!("ListOffsets routing must request at most the orders topic"),
    };

    let mut response = MetadataResponse::default();
    response.controller_id = 1;
    response.brokers = vec![
        metadata_broker(1, bootstrap_port),
        metadata_broker(2, leader_port),
    ];
    if include_orders {
        response.topics = vec![orders_topic()];
    }
    encoded_response::<MetadataRequest, _>(request.correlation_id, &response, request.api_version)
}

fn describe_cluster(request: &RequestFrame, bootstrap_port: u16, leader_port: u16) -> Vec<u8> {
    let decoded: DescribeClusterRequest = request.decode();
    assert!(!decoded.include_cluster_authorized_operations);
    assert_eq!(decoded.endpoint_type, 1);
    assert!(!decoded.include_fenced_brokers);

    let mut response = DescribeClusterResponse::default();
    response.cluster_id = "admin-list-offsets-loopback".into();
    response.controller_id = 1;
    response.brokers = vec![
        cluster_broker(1, bootstrap_port),
        cluster_broker(2, leader_port),
    ];
    encoded_response::<DescribeClusterRequest, _>(
        request.correlation_id,
        &response,
        request.api_version,
    )
}

fn list_offsets(request: &RequestFrame) -> Vec<u8> {
    let decoded: ListOffsetsRequest = request.decode();
    let [topic] = decoded.topics.as_slice() else {
        panic!("Admin ListOffsets request must contain one topic")
    };
    let [partition] = topic.partitions.as_slice() else {
        panic!("Admin ListOffsets request must contain one partition")
    };
    let (offset, timestamp, leader_epoch, throttle_time_ms) = match partition.timestamp {
        -3 => (71, 1_700, 3, 7),
        -4 => (10, -1, 4, 8),
        -5 => (-1, -1, 5, 9),
        -6 => (81, -1, 6, 11),
        other => panic!("unexpected Kafka 4.3 ListOffsets selector {other}"),
    };

    let mut partition_response = ListOffsetsPartitionResponse::default();
    partition_response.partition_index = partition.partition_index;
    partition_response.timestamp = timestamp;
    partition_response.offset = offset;
    partition_response.leader_epoch = leader_epoch;
    let mut topic_response = ListOffsetsTopicResponse::default();
    topic_response.name = topic.name.clone();
    topic_response.partitions = vec![partition_response];
    let mut response = ListOffsetsResponse::default();
    response.throttle_time_ms = throttle_time_ms;
    response.topics = vec![topic_response];
    encoded_response::<ListOffsetsRequest, _>(
        request.correlation_id,
        &response,
        request.api_version,
    )
}

fn orders_topic() -> MetadataResponseTopic {
    let mut topic = MetadataResponseTopic::default();
    topic.name = Some("orders".into());
    topic.partitions = (0..4).map(orders_partition).collect();
    topic
}

fn orders_partition(index: i32) -> MetadataResponsePartition {
    let mut partition = MetadataResponsePartition::default();
    partition.partition_index = index;
    partition.leader_id = 2;
    partition.replica_nodes = vec![2];
    partition.isr_nodes = vec![2];
    partition
}

fn metadata_broker(node_id: i32, port: u16) -> MetadataResponseBroker {
    let mut broker = MetadataResponseBroker::default();
    broker.node_id = node_id;
    broker.host = "127.0.0.1".into();
    broker.port = i32::from(port);
    broker
}

fn cluster_broker(broker_id: i32, port: u16) -> DescribeClusterBroker {
    let mut broker = DescribeClusterBroker::default();
    broker.broker_id = broker_id;
    broker.host = "127.0.0.1".into();
    broker.port = i32::from(port);
    broker
}

fn advertised<R: KafkaRequest>(minimum: i16, maximum: i16) -> AdvertisedApi {
    let mut advertised = AdvertisedApi::default();
    advertised.api_key = R::API_KEY.value();
    advertised.min_version = minimum;
    advertised.max_version = maximum;
    advertised
}
