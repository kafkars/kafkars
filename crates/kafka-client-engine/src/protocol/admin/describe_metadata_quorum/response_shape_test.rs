//! Hostile shape, uniqueness, version, scalar, and retained-limit evidence.

use kafka_wire::{
    DescribeQuorumResponse,
    describe_quorum_response::{Listener, Node, PartitionData, ReplicaState, TopicData},
};
use kafka_wire_core::Uuid;

use super::{
    DescribeMetadataQuorumProtocolFailure as Failure, normalize_describe_metadata_quorum_response,
    request::METADATA_TOPIC,
};

const LIMIT: usize = 4 * 1024 * 1024;

#[test]
fn success_requires_exact_fixed_topic_and_partition_shape() {
    let mut response = success_response();
    response.topics.clear();
    assert_eq!(
        failure(2, &response),
        Failure::UnexpectedTopicCount { actual: 0 }
    );

    let mut response = success_response();
    response.topics[0].topic_name = "other".into();
    assert_eq!(failure(2, &response), Failure::UnexpectedTopicName);

    let mut response = success_response();
    response.topics[0].partitions[0].partition_index = 1;
    assert_eq!(
        failure(2, &response),
        Failure::UnexpectedPartition { actual: 1 }
    );
}

#[test]
fn replica_ids_are_unique_within_and_across_sorted_roles() {
    let mut response = success_response();
    response.topics[0].partitions[0].current_voters = vec![replica(2), replica(2)];
    response.topics[0].partitions[0].leader_id = 2;
    assert_eq!(
        failure(2, &response),
        Failure::DuplicateReplicaId { actual: 2 }
    );

    let mut response = success_response();
    response.topics[0].partitions[0].observers = vec![replica(1)];
    assert_eq!(
        failure(2, &response),
        Failure::ReplicaInBothRoles { actual: 1 }
    );
}

#[test]
fn nodes_and_listener_names_are_unique_after_canonical_ordering() {
    let mut response = success_response();
    response.nodes = vec![node(2, vec![listener("a")]), node(2, vec![listener("b")])];
    assert_eq!(
        failure(2, &response),
        Failure::DuplicateNodeId { actual: 2 }
    );

    let mut response = success_response();
    response.nodes = vec![node(2, vec![listener("a"), listener("a")])];
    assert_eq!(
        failure(2, &response),
        Failure::DuplicateListenerName { node_id: 2 }
    );
}

#[test]
fn selected_version_rejects_nonrepresentable_generated_defaults() {
    let mut response = success_response();
    response.nodes = vec![node(2, vec![listener("a")])];
    assert_eq!(
        failure(1, &response),
        Failure::FieldNotRepresentable { field: "nodes" }
    );

    let mut response = success_response();
    response.topics[0].partitions[0].current_voters[0].replica_directory_id =
        Uuid::from_bytes([1; 16]);
    assert_eq!(
        failure(1, &response),
        Failure::FieldNotRepresentable {
            field: "replica_directory_id"
        }
    );

    let mut response = success_response();
    response.topics[0].partitions[0].current_voters[0].last_fetch_timestamp = 1;
    assert_eq!(
        failure(0, &response),
        Failure::FieldNotRepresentable {
            field: "replica timestamps"
        }
    );
}

#[test]
fn invalid_scalars_and_missing_leader_voter_are_rejected() {
    let mut response = success_response();
    response.topics[0].partitions[0].leader_epoch = -1;
    assert_eq!(
        failure(2, &response),
        Failure::InvalidSentinel {
            field: "leader_epoch",
            actual: -1
        }
    );

    let mut response = success_response();
    response.topics[0].partitions[0].current_voters[0].log_end_offset = -2;
    assert_eq!(
        failure(2, &response),
        Failure::InvalidSentinel {
            field: "log_end_offset",
            actual: -2
        }
    );

    let mut response = success_response();
    response.topics[0].partitions[0].leader_id = 7;
    assert_eq!(failure(2, &response), Failure::LeaderNotVoter { actual: 7 });
}

#[test]
fn listener_shape_and_result_capacity_are_bounded() {
    let mut response = success_response();
    response.nodes = vec![node(2, vec![listener("")])];
    assert_eq!(
        failure(2, &response),
        Failure::EmptyString {
            field: "listener_name"
        }
    );

    assert!(matches!(
        normalize_describe_metadata_quorum_response(2, &success_response(), 0),
        Err(Failure::RetainedBytes { .. })
    ));
}

#[test]
fn replica_and_listener_counts_have_hard_limits() {
    let mut response = success_response();
    response.topics[0].partitions[0].current_voters = (0..=1024).map(replica).collect::<Vec<_>>();
    response.topics[0].partitions[0].leader_id = 0;
    assert_eq!(
        failure(2, &response),
        Failure::TooMany {
            field: "current_voters",
            actual: 1025,
            max: 1024
        }
    );

    let mut response = success_response();
    response.nodes = vec![node(
        1,
        (0..65)
            .map(|index| listener(&format!("listener-{index}")))
            .collect(),
    )];
    assert_eq!(
        failure(2, &response),
        Failure::TooMany {
            field: "listeners",
            actual: 65,
            max: 64
        }
    );
}

fn failure(version: i16, response: &DescribeQuorumResponse) -> Failure {
    normalize_describe_metadata_quorum_response(version, response, LIMIT).map_or_else(
        |error| error,
        |value| panic!("hostile shape must fail: {value:?}"),
    )
}

fn success_response() -> DescribeQuorumResponse {
    let mut partition = PartitionData::default();
    partition.partition_index = 0;
    partition.leader_id = 1;
    partition.leader_epoch = 2;
    partition.high_watermark = 3;
    partition.current_voters = vec![replica(1)];
    let mut topic = TopicData::default();
    topic.topic_name = METADATA_TOPIC.into();
    topic.partitions = vec![partition];
    let mut response = DescribeQuorumResponse::default();
    response.topics = vec![topic];
    response
}

fn replica(id: i32) -> ReplicaState {
    let mut replica = ReplicaState::default();
    replica.replica_id = id;
    replica.log_end_offset = 0;
    replica
}

fn listener(name: &str) -> Listener {
    let mut listener = Listener::default();
    listener.name = name.into();
    listener.host = "host".into();
    listener.port = 9093;
    listener
}

fn node(id: i32, listeners: Vec<Listener>) -> Node {
    let mut node = Node::default();
    node.node_id = id;
    node.listeners = listeners;
    node
}
