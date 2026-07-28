//! Generated topology and deliberately reordered API 45 v1 responses.

use kafka_wire::{
    AlterPartitionReassignmentsRequest, AlterPartitionReassignmentsResponse, ApiVersionsRequest,
    ApiVersionsResponse, DescribeClusterRequest, DescribeClusterResponse, KafkaRequest,
    MetadataRequest, MetadataResponse,
    alter_partition_reassignments_response::{
        ReassignablePartitionResponse, ReassignableTopicResponse,
    },
    api_versions_response::ApiVersion as AdvertisedApi,
    describe_cluster_response::DescribeClusterBroker,
    metadata_response::MetadataResponseBroker,
};

use super::{
    frame::{RequestFrame, encoded_response},
    observation::Workflow,
};

pub(super) const METADATA: i16 = 3;
const API_VERSIONS: i16 = 18;
pub(super) const ALTER_PARTITION_REASSIGNMENTS: i16 = 45;
const DESCRIBE_CLUSTER: i16 = 60;

#[derive(Clone, Copy)]
pub(super) struct BrokerPorts {
    pub(super) bootstrap: u16,
    pub(super) controller: u16,
}

pub(super) fn for_request(
    request: &RequestFrame,
    node_id: i32,
    ports: BrokerPorts,
    workflow: Workflow,
    controller_refreshed: bool,
) -> Vec<u8> {
    match request.api_key {
        API_VERSIONS => api_versions(request),
        METADATA => metadata(request, ports),
        DESCRIBE_CLUSTER => describe_cluster(request, ports),
        ALTER_PARTITION_REASSIGNMENTS if node_id == 7 => {
            alter_reassignments(request, workflow, controller_refreshed)
        }
        ALTER_PARTITION_REASSIGNMENTS => {
            panic!("API 45 must route to controller 7, not broker {node_id}")
        }
        other => panic!("unexpected AlterPartitionReassignments API key {other}"),
    }
}

fn api_versions(request: &RequestFrame) -> Vec<u8> {
    let _decoded: ApiVersionsRequest = request.decode();
    let mut response = ApiVersionsResponse::default();
    response.api_keys = vec![
        advertised::<ApiVersionsRequest>(0, 0),
        advertised::<MetadataRequest>(4, 13),
        advertised::<AlterPartitionReassignmentsRequest>(0, i16::MAX),
        advertised::<DescribeClusterRequest>(0, 2),
    ];
    encoded_response::<ApiVersionsRequest, _>(
        request.correlation_id,
        &response,
        request.api_version,
    )
}

fn metadata(request: &RequestFrame, ports: BrokerPorts) -> Vec<u8> {
    let decoded: MetadataRequest = request.decode();
    assert!(
        decoded.topics.as_ref().is_none_or(Vec::is_empty),
        "controller discovery must not request topic metadata"
    );
    let mut response = MetadataResponse::default();
    response.cluster_id = Some("partition-reassignments-loopback".into());
    response.controller_id = 7;
    response.brokers = vec![
        metadata_broker(2, ports.bootstrap),
        metadata_broker(7, ports.controller),
    ];
    encoded_response::<MetadataRequest, _>(request.correlation_id, &response, request.api_version)
}

fn describe_cluster(request: &RequestFrame, ports: BrokerPorts) -> Vec<u8> {
    let decoded: DescribeClusterRequest = request.decode();
    assert!(!decoded.include_cluster_authorized_operations);
    assert_eq!(decoded.endpoint_type, 1);
    assert!(!decoded.include_fenced_brokers);
    let mut response = DescribeClusterResponse::default();
    response.cluster_id = "partition-reassignments-loopback".into();
    response.controller_id = 7;
    response.brokers = vec![
        cluster_broker(2, ports.bootstrap),
        cluster_broker(7, ports.controller),
    ];
    encoded_response::<DescribeClusterRequest, _>(
        request.correlation_id,
        &response,
        request.api_version,
    )
}

fn alter_reassignments(
    request: &RequestFrame,
    workflow: Workflow,
    controller_refreshed: bool,
) -> Vec<u8> {
    assert_eq!(
        request.api_version.value(),
        1,
        "client ceiling and explicit false policy must select API 45 v1"
    );
    let response = match workflow {
        Workflow::Standard => standard_response(),
        Workflow::ControllerRecovery if controller_refreshed => recovered_response(),
        Workflow::ControllerRecovery => not_controller_response(),
    };
    encoded_response::<AlterPartitionReassignmentsRequest, _>(
        request.correlation_id,
        &response,
        request.api_version,
    )
}

fn standard_response() -> AlterPartitionReassignmentsResponse {
    let mut response = base_response(43);
    response.responses = vec![
        topic("beta", vec![partition(4, 0, None)]),
        topic("zeta", vec![partition(0, 0, None), partition(2, 0, None)]),
        topic(
            "alpha",
            vec![partition(3, -31_998, Some("controller-denied"))],
        ),
    ];
    response
}

fn not_controller_response() -> AlterPartitionReassignmentsResponse {
    let mut response = base_response(47);
    response.error_code = 41;
    response.error_message = Some("stale-controller".into());
    response
}

fn recovered_response() -> AlterPartitionReassignmentsResponse {
    let mut response = base_response(53);
    response.responses = vec![
        topic("alpha", vec![partition(3, 0, None)]),
        topic("beta", vec![partition(4, 0, None)]),
        topic("zeta", vec![partition(2, 0, None), partition(0, 0, None)]),
    ];
    response
}

fn base_response(throttle_time_ms: i32) -> AlterPartitionReassignmentsResponse {
    let mut response = AlterPartitionReassignmentsResponse::default();
    response.throttle_time_ms = throttle_time_ms;
    response.allow_replication_factor_change = false;
    response.error_message = None;
    response
}

fn topic(name: &str, partitions: Vec<ReassignablePartitionResponse>) -> ReassignableTopicResponse {
    let mut topic = ReassignableTopicResponse::default();
    topic.name = name.into();
    topic.partitions = partitions;
    topic
}

fn partition(
    partition_index: i32,
    error_code: i16,
    error_message: Option<&str>,
) -> ReassignablePartitionResponse {
    let mut partition = ReassignablePartitionResponse::default();
    partition.partition_index = partition_index;
    partition.error_code = error_code;
    partition.error_message = error_message.map(Into::into);
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
