//! Stable generated-free metadata-quorum value tests.
#![expect(
    clippy::expect_used,
    reason = "the test asserts a required optional value"
)]

use super::{
    MetadataQuorumDescription, MetadataQuorumListener, MetadataQuorumNode, MetadataQuorumReplica,
};

#[test]
fn description_preserves_explicit_absence_and_nested_values() {
    let voter = MetadataQuorumReplica::new(3, Some([7; 16]), Some(41), None, Some(39));
    let observer = MetadataQuorumReplica::new(8, None, None, None, None);
    let listener =
        MetadataQuorumListener::new("CONTROLLER".into(), "controller.local".into(), 9093);
    let node = MetadataQuorumNode::new(3, vec![listener]);
    let description = MetadataQuorumDescription::new(
        Some(3),
        12,
        40,
        vec![voter],
        vec![observer],
        Some(vec![node]),
    );

    assert_eq!(description.leader_id(), Some(3));
    assert_eq!(description.leader_epoch(), 12);
    assert_eq!(description.high_watermark(), 40);
    assert_eq!(
        description.voters()[0].replica_directory_id(),
        Some([7; 16])
    );
    assert_eq!(description.voters()[0].last_fetch_timestamp_ms(), None);
    assert_eq!(description.observers()[0].log_end_offset(), None);
    let nodes = description.nodes().expect("v2 nodes remain present");
    assert_eq!(nodes[0].node_id(), 3);
    assert_eq!(nodes[0].listeners()[0].name(), "CONTROLLER");
    assert_eq!(nodes[0].listeners()[0].host(), "controller.local");
    assert_eq!(nodes[0].listeners()[0].port(), 9093);
}

#[test]
fn unrepresented_nodes_remain_distinct_from_a_represented_empty_set() {
    let absent = MetadataQuorumDescription::new(None, 0, 0, Vec::new(), Vec::new(), None);
    let present =
        MetadataQuorumDescription::new(None, 0, 0, Vec::new(), Vec::new(), Some(Vec::new()));

    assert_eq!(absent.nodes(), None);
    assert_eq!(present.nodes(), Some([].as_slice()));
}
