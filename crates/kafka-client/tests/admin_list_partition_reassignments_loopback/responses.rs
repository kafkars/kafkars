//! Generated discovery and scripted API 46 v0 controller responses.

use kafka_wire::{
    ApiVersionsRequest, ApiVersionsResponse, DescribeClusterRequest, DescribeClusterResponse,
    KafkaRequest, ListPartitionReassignmentsRequest, ListPartitionReassignmentsResponse,
    MetadataRequest, MetadataResponse,
    api_versions_response::ApiVersion as AdvertisedApi,
    describe_cluster_response::DescribeClusterBroker,
    list_partition_reassignments_response::{
        OngoingPartitionReassignment, OngoingTopicReassignment,
    },
    metadata_response::MetadataResponseBroker,
};

use super::{
    frame::{RequestFrame, encoded_response},
    observation::Workflow,
};

pub(super) const METADATA: i16 = 3;
const API_VERSIONS: i16 = 18;
pub(super) const LIST_PARTITION_REASSIGNMENTS: i16 = 46;
const DESCRIBE_CLUSTER: i16 = 60;

pub(super) fn for_request(
    request: &RequestFrame,
    node_id: i32,
    broker_2_port: u16,
    broker_7_port: u16,
    workflow: Workflow,
    controller_refreshed: bool,
) -> Vec<u8> {
    match request.api_key {
        API_VERSIONS => api_versions(request),
        METADATA => metadata(request, broker_2_port, broker_7_port),
        DESCRIBE_CLUSTER => describe_cluster(request, broker_2_port, broker_7_port),
        LIST_PARTITION_REASSIGNMENTS if node_id == 7 => {
            list(request, workflow, controller_refreshed)
        }
        LIST_PARTITION_REASSIGNMENTS => {
            panic!("API 46 must route to controller 7, not broker {node_id}")
        }
        other => panic!("unexpected ListPartitionReassignments Kafka API key {other}"),
    }
}

fn api_versions(request: &RequestFrame) -> Vec<u8> {
    let _decoded: ApiVersionsRequest = request.decode();
    let mut response = ApiVersionsResponse::default();
    response.api_keys = vec![
        advertised::<ApiVersionsRequest>(0, 0),
        advertised::<MetadataRequest>(4, 13),
        advertised::<ListPartitionReassignmentsRequest>(0, i16::MAX),
        advertised::<DescribeClusterRequest>(0, 2),
    ];
    encoded_response::<ApiVersionsRequest, _>(
        request.correlation_id,
        &response,
        request.api_version,
    )
}

fn metadata(request: &RequestFrame, broker_2_port: u16, broker_7_port: u16) -> Vec<u8> {
    let decoded: MetadataRequest = request.decode();
    assert!(
        matches!(decoded.topics.as_ref(), Some(topics) if topics.is_empty()),
        "controller discovery must use an empty topic array"
    );
    let mut response = MetadataResponse::default();
    response.cluster_id = Some("list-partition-reassignments-loopback".into());
    response.controller_id = 7;
    response.brokers = vec![
        metadata_broker(2, broker_2_port),
        metadata_broker(7, broker_7_port),
    ];
    encoded_response::<MetadataRequest, _>(request.correlation_id, &response, request.api_version)
}

fn describe_cluster(request: &RequestFrame, broker_2_port: u16, broker_7_port: u16) -> Vec<u8> {
    let decoded: DescribeClusterRequest = request.decode();
    assert!(!decoded.include_cluster_authorized_operations);
    assert_eq!(decoded.endpoint_type, 1);
    assert!(!decoded.include_fenced_brokers);
    let mut response = DescribeClusterResponse::default();
    response.cluster_id = "list-partition-reassignments-loopback".into();
    response.controller_id = 7;
    response.brokers = vec![
        cluster_broker(2, broker_2_port),
        cluster_broker(7, broker_7_port),
    ];
    encoded_response::<DescribeClusterRequest, _>(
        request.correlation_id,
        &response,
        request.api_version,
    )
}

fn list(request: &RequestFrame, workflow: Workflow, controller_refreshed: bool) -> Vec<u8> {
    assert_eq!(
        request.api_version.value(),
        0,
        "client must retain its API 46 v0 ceiling"
    );
    let response = match workflow {
        Workflow::Selected => selected_response(),
        Workflow::AllActive => all_active_response(),
        Workflow::BrokerError => broker_error_response(),
        Workflow::ControllerRecovery if controller_refreshed => all_active_response(),
        Workflow::ControllerRecovery => not_controller_response(),
    };
    encoded_response::<ListPartitionReassignmentsRequest, _>(
        request.correlation_id,
        &response,
        request.api_version,
    )
}

fn selected_response() -> ListPartitionReassignmentsResponse {
    let mut response = ListPartitionReassignmentsResponse::default();
    response.throttle_time_ms = 31;
    response.error_message = None;
    response.topics = vec![
        topic("alpha", vec![partition(0, &[2, 7], &[7], &[])]),
        topic(
            "zeta",
            vec![
                partition(1, &[7, 9], &[9], &[2]),
                partition(2, &[7, 2, 9], &[9], &[1]),
            ],
        ),
    ];
    response
}

fn all_active_response() -> ListPartitionReassignmentsResponse {
    let mut response = ListPartitionReassignmentsResponse::default();
    response.throttle_time_ms = 37;
    response.error_message = None;
    response.topics = vec![
        topic(
            "zeta",
            vec![
                partition(2, &[9, 7], &[7], &[2]),
                partition(0, &[7, 9], &[9], &[2]),
            ],
        ),
        topic("alpha", vec![partition(1, &[2, 7], &[7], &[])]),
    ];
    response
}

fn broker_error_response() -> ListPartitionReassignmentsResponse {
    let mut response = ListPartitionReassignmentsResponse::default();
    response.throttle_time_ms = 43;
    response.error_code = -31_999;
    response.error_message = Some("reassignment-listing-denied".into());
    response
}

fn not_controller_response() -> ListPartitionReassignmentsResponse {
    let mut response = ListPartitionReassignmentsResponse::default();
    response.throttle_time_ms = 47;
    response.error_code = 41;
    response.error_message = Some("stale-controller".into());
    response
}

fn topic(name: &str, partitions: Vec<OngoingPartitionReassignment>) -> OngoingTopicReassignment {
    let mut topic = OngoingTopicReassignment::default();
    topic.name = name.into();
    topic.partitions = partitions;
    topic
}

fn partition(
    partition_index: i32,
    replicas: &[i32],
    adding_replicas: &[i32],
    removing_replicas: &[i32],
) -> OngoingPartitionReassignment {
    let mut partition = OngoingPartitionReassignment::default();
    partition.partition_index = partition_index;
    partition.replicas = replicas.to_vec();
    partition.adding_replicas = adding_replicas.to_vec();
    partition.removing_replicas = removing_replicas.to_vec();
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
