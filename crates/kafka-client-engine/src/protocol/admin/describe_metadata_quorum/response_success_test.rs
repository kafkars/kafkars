//! Successful selected-version normalization and canonical-order evidence.

use kafka_wire::{
    DescribeQuorumResponse,
    describe_quorum_response::{Listener, Node, PartitionData, ReplicaState, TopicData},
};
use kafka_wire_core::Uuid;

use super::{
    NormalizedMetadataQuorumOutcome, normalize_describe_metadata_quorum_response,
    request::METADATA_TOPIC,
};

const LIMIT: usize = 4 * 1024 * 1024;

#[test]
fn v0_maps_documented_sentinels_and_orders_replica_ids() {
    let mut response = success_response();
    response.topics[0].partitions[0].leader_id = -1;
    response.topics[0].partitions[0].current_voters = vec![replica(7), replica(2)];
    response.topics[0].partitions[0].observers = vec![replica(9)];

    let quorum = quorum(0, &response);
    let (leader, epoch, high_watermark, voters, observers, nodes) = quorum.into_parts();
    assert_eq!(leader, None);
    assert_eq!((epoch, high_watermark), (3, 41));
    assert!(nodes.is_none());
    assert_eq!(voters[0].clone().into_parts().0, 2);
    assert_eq!(voters[1].clone().into_parts().0, 7);
    assert_eq!(
        voters[0].clone().into_parts(),
        (2, None, Some(12), None, None)
    );
    assert_eq!(observers[0].clone().into_parts().0, 9);
}

#[test]
fn v1_retains_timestamps_but_not_directory_ids_or_nodes() {
    let mut response = success_response();
    let mut voter = replica(1);
    voter.last_fetch_timestamp = 100;
    voter.last_caught_up_timestamp = -1;
    response.topics[0].partitions[0].current_voters = vec![voter];

    let quorum = quorum(1, &response);
    let (_, _, _, voters, _, nodes) = quorum.into_parts();
    assert_eq!(
        voters[0].clone().into_parts(),
        (1, None, Some(12), Some(100), None)
    );
    assert!(nodes.is_none());
}

#[test]
fn v2_retains_nonzero_directory_and_orders_nodes_and_listener_names() {
    let mut response = success_response();
    let mut voter = replica(1);
    voter.replica_directory_id = Uuid::from_bytes([7; 16]);
    response.topics[0].partitions[0].current_voters = vec![voter];
    response.nodes = vec![
        node(
            7,
            vec![listener("z", "z.example"), listener("a", "a.example")],
        ),
        node(2, vec![listener("control", "c.example")]),
    ];

    let quorum = quorum(2, &response);
    let (_, _, _, voters, _, nodes) = quorum.into_parts();
    assert_eq!(voters[0].clone().into_parts().1, Some([7; 16]));
    let nodes = nodes.unwrap_or_else(|| panic!("v2 nodes should be present"));
    let (first_id, _) = nodes[0].clone().into_parts();
    let (second_id, listeners) = nodes[1].clone().into_parts();
    assert_eq!((first_id, second_id), (2, 7));
    assert_eq!(listeners[0].clone().into_parts().0, "a");
    assert_eq!(listeners[1].clone().into_parts().0, "z");
}

#[test]
fn v2_zero_directory_is_explicit_absence() {
    let response = success_response();
    let quorum = quorum(2, &response);
    let (_, _, _, voters, _, nodes) = quorum.into_parts();
    assert_eq!(voters[0].clone().into_parts().1, None);
    assert_eq!(nodes, Some(Vec::new()));
}

fn success_response() -> DescribeQuorumResponse {
    let mut partition = PartitionData::default();
    partition.partition_index = 0;
    partition.leader_id = 1;
    partition.leader_epoch = 3;
    partition.high_watermark = 41;
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
    replica.log_end_offset = 12;
    replica
}

fn listener(name: &str, host: &str) -> Listener {
    let mut listener = Listener::default();
    listener.name = name.into();
    listener.host = host.into();
    listener.port = 9093;
    listener
}

fn node(id: i32, listeners: Vec<Listener>) -> Node {
    let mut node = Node::default();
    node.node_id = id;
    node.listeners = listeners;
    node
}

fn quorum(version: i16, response: &DescribeQuorumResponse) -> super::NormalizedMetadataQuorum {
    let normalized = normalize_describe_metadata_quorum_response(version, response, LIMIT)
        .unwrap_or_else(|error| panic!("valid response: {error:?}"));
    let (outcome, retained) = normalized.into_parts();
    assert!(retained > 0);
    let NormalizedMetadataQuorumOutcome::Quorum(quorum) = outcome else {
        panic!("expected quorum");
    };
    quorum
}
