//! Bounded normalized quorum-description value scenarios.

use super::{
    DESCRIBE_METADATA_QUORUM_MAX_LISTENERS_PER_NODE, DESCRIBE_METADATA_QUORUM_MAX_NODES,
    DESCRIBE_METADATA_QUORUM_MAX_REPLICAS, DescribeMetadataQuorumDescription,
    DescribeMetadataQuorumListener, DescribeMetadataQuorumNode, DescribeMetadataQuorumReplica,
    DescribeMetadataQuorumValueError,
};

#[test]
fn description_preserves_normalized_scalar_optional_and_version_facts() {
    let voter = replica(1, Some([1; 16]), Some(44), None, Some(45));
    let observer = replica(2, None, None, None, None);
    let listener = DescribeMetadataQuorumListener::new(
        "CONTROLLER".to_owned(),
        "controller.example".to_owned(),
        9093,
    );
    let node = DescribeMetadataQuorumNode::new(1, vec![listener]);
    let description = DescribeMetadataQuorumDescription::new(
        Some(1),
        7,
        42,
        vec![voter],
        vec![observer],
        Some(vec![node]),
    )
    .unwrap_or_else(|error| panic!("valid description: {error}"));

    assert_eq!(description.leader_id(), Some(1));
    assert_eq!(description.leader_epoch(), 7);
    assert_eq!(description.high_watermark(), 42);
    assert_eq!(
        description.voters()[0].replica_directory_id(),
        Some([1; 16])
    );
    assert_eq!(description.voters()[0].last_fetch_timestamp_ms(), None);
    assert_eq!(description.observers()[0].log_end_offset(), None);
    let nodes = description.nodes().unwrap_or_else(|| panic!("v2 nodes"));
    assert_eq!(nodes[0].node_id(), 1);
    assert_eq!(nodes[0].listeners()[0].name(), "CONTROLLER");
    assert_eq!(nodes[0].listeners()[0].host(), "controller.example");
    assert_eq!(nodes[0].listeners()[0].port(), 9093);
}

#[test]
fn pre_v2_node_absence_remains_distinct_from_represented_empty_nodes() {
    let absent = description(Vec::new(), Vec::new(), None)
        .unwrap_or_else(|error| panic!("absent nodes: {error}"));
    let represented = description(Vec::new(), Vec::new(), Some(Vec::new()))
        .unwrap_or_else(|error| panic!("represented nodes: {error}"));

    assert_eq!(absent.nodes(), None);
    assert_eq!(represented.nodes(), Some([].as_slice()));
}

#[test]
fn scalar_and_replica_sentinel_invariants_are_enforced() {
    let cases = [
        (
            DescribeMetadataQuorumDescription::new(Some(-2), 1, 1, Vec::new(), Vec::new(), None),
            DescribeMetadataQuorumValueError::NegativeLeaderId,
        ),
        (
            DescribeMetadataQuorumDescription::new(None, -1, 1, Vec::new(), Vec::new(), None),
            DescribeMetadataQuorumValueError::NegativeLeaderEpoch,
        ),
        (
            DescribeMetadataQuorumDescription::new(None, 1, -1, Vec::new(), Vec::new(), None),
            DescribeMetadataQuorumValueError::NegativeHighWatermark,
        ),
        (
            description(vec![replica(-1, None, None, None, None)], Vec::new(), None),
            DescribeMetadataQuorumValueError::NegativeReplicaId,
        ),
        (
            description(
                vec![replica(1, Some([0; 16]), None, None, None)],
                Vec::new(),
                None,
            ),
            DescribeMetadataQuorumValueError::ZeroReplicaDirectoryId,
        ),
        (
            description(
                vec![replica(1, None, Some(-1), None, None)],
                Vec::new(),
                None,
            ),
            DescribeMetadataQuorumValueError::NegativeReplicaOffset,
        ),
        (
            description(
                vec![replica(1, None, None, Some(-1), None)],
                Vec::new(),
                None,
            ),
            DescribeMetadataQuorumValueError::NegativeReplicaTimestamp,
        ),
    ];

    for (actual, expected) in cases {
        assert_eq!(actual, Err(expected));
    }
}

#[test]
fn replica_roles_require_strict_order_uniqueness_and_disjointness() {
    assert_eq!(
        description(
            vec![
                replica(2, None, None, None, None),
                replica(1, None, None, None, None)
            ],
            Vec::new(),
            None,
        ),
        Err(DescribeMetadataQuorumValueError::NonCanonicalVoterOrder)
    );
    assert_eq!(
        description(
            Vec::new(),
            vec![
                replica(1, None, None, None, None),
                replica(1, None, None, None, None)
            ],
            None,
        ),
        Err(DescribeMetadataQuorumValueError::NonCanonicalObserverOrder)
    );
    assert_eq!(
        description(
            vec![replica(1, None, None, None, None)],
            vec![replica(1, None, None, None, None)],
            None,
        ),
        Err(DescribeMetadataQuorumValueError::ReplicaRoleOverlap)
    );
}

#[test]
fn node_and_listener_shapes_are_bounded_and_canonical() {
    let listener = |name: &str, host: &str, port| {
        DescribeMetadataQuorumListener::new(name.to_owned(), host.to_owned(), port)
    };
    assert_eq!(
        description(
            Vec::new(),
            Vec::new(),
            Some(vec![DescribeMetadataQuorumNode::new(
                1,
                vec![listener("Z", "z", 1), listener("A", "a", 2)],
            )]),
        ),
        Err(DescribeMetadataQuorumValueError::NonCanonicalListenerOrder)
    );
    for (endpoint, expected) in [
        (
            listener("", "host", 1),
            DescribeMetadataQuorumValueError::EmptyListenerName,
        ),
        (
            listener("name", "", 1),
            DescribeMetadataQuorumValueError::EmptyListenerHost,
        ),
        (
            listener("name", "host", 0),
            DescribeMetadataQuorumValueError::ZeroListenerPort,
        ),
    ] {
        assert_eq!(
            description(
                Vec::new(),
                Vec::new(),
                Some(vec![DescribeMetadataQuorumNode::new(1, vec![endpoint])]),
            ),
            Err(expected)
        );
    }
    assert_eq!(
        description(
            Vec::new(),
            Vec::new(),
            Some(vec![
                DescribeMetadataQuorumNode::new(2, Vec::new()),
                DescribeMetadataQuorumNode::new(1, Vec::new()),
            ]),
        ),
        Err(DescribeMetadataQuorumValueError::NonCanonicalNodeOrder)
    );
    assert_eq!(
        description(
            Vec::new(),
            Vec::new(),
            Some(vec![DescribeMetadataQuorumNode::new(-1, Vec::new())]),
        ),
        Err(DescribeMetadataQuorumValueError::NegativeNodeId)
    );

    let too_long = "x".repeat(i16::MAX as usize + 1);
    for (endpoint, expected) in [
        (
            listener(&too_long, "host", 1),
            DescribeMetadataQuorumValueError::ListenerNameTooLong,
        ),
        (
            listener("name", &too_long, 1),
            DescribeMetadataQuorumValueError::ListenerHostTooLong,
        ),
    ] {
        assert_eq!(
            description(
                Vec::new(),
                Vec::new(),
                Some(vec![DescribeMetadataQuorumNode::new(1, vec![endpoint])]),
            ),
            Err(expected)
        );
    }
}

#[test]
fn collection_caps_reject_oversized_normalized_values() {
    let too_many_replicas =
        vec![replica(1, None, None, None, None); DESCRIBE_METADATA_QUORUM_MAX_REPLICAS + 1];
    assert_eq!(
        description(too_many_replicas, Vec::new(), None),
        Err(DescribeMetadataQuorumValueError::TooManyVoters)
    );
    let too_many_observers =
        vec![replica(1, None, None, None, None); DESCRIBE_METADATA_QUORUM_MAX_REPLICAS + 1];
    assert_eq!(
        description(Vec::new(), too_many_observers, None),
        Err(DescribeMetadataQuorumValueError::TooManyObservers)
    );

    let nodes = vec![
        DescribeMetadataQuorumNode::new(1, Vec::new());
        DESCRIBE_METADATA_QUORUM_MAX_NODES + 1
    ];
    assert_eq!(
        description(Vec::new(), Vec::new(), Some(nodes)),
        Err(DescribeMetadataQuorumValueError::TooManyNodes)
    );

    let listeners = vec![
        DescribeMetadataQuorumListener::new("A".to_owned(), "host".to_owned(), 1);
        DESCRIBE_METADATA_QUORUM_MAX_LISTENERS_PER_NODE + 1
    ];
    assert_eq!(
        description(
            Vec::new(),
            Vec::new(),
            Some(vec![DescribeMetadataQuorumNode::new(1, listeners)]),
        ),
        Err(DescribeMetadataQuorumValueError::TooManyListeners)
    );
}

fn description(
    voters: Vec<DescribeMetadataQuorumReplica>,
    observers: Vec<DescribeMetadataQuorumReplica>,
    nodes: Option<Vec<DescribeMetadataQuorumNode>>,
) -> Result<DescribeMetadataQuorumDescription, DescribeMetadataQuorumValueError> {
    DescribeMetadataQuorumDescription::new(None, 1, 1, voters, observers, nodes)
}

fn replica(
    id: i32,
    directory_id: Option<[u8; 16]>,
    offset: Option<i64>,
    fetched: Option<i64>,
    caught_up: Option<i64>,
) -> DescribeMetadataQuorumReplica {
    DescribeMetadataQuorumReplica::new(id, directory_id, offset, fetched, caught_up)
}
