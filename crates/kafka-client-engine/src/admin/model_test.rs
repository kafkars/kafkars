//! Canonical bounded-storage scenarios for public `CreateTopics` request values.

use kafka_client_core::CreateTopicPlacement;

use super::{CreateTopic, CreateTopicConfig, CreateTopicReplicaAssignment, CreateTopicsRequest};

#[test]
fn excess_input_capacity_is_removed_before_retained_byte_charging() {
    let mut name = String::with_capacity(1024 * 1024);
    name.push_str("orders");
    let mut value = String::with_capacity(512 * 1024);
    value.push_str("compact");
    let mut topics = Vec::with_capacity(4096);
    topics.push(
        CreateTopic::new(name, 3)
            .with_config(CreateTopicConfig::new("cleanup.policy", Some(value))),
    );
    let request = CreateTopicsRequest::new(topics);
    assert!(!request.storage_is_canonical());

    let canonical = request.canonicalize();
    assert!(canonical.storage_is_canonical());
    assert!(
        canonical
            .retained_charge()
            .is_some_and(|charge| charge < 32 * 1024)
    );
}

#[test]
fn manual_assignments_are_canonicalized_charged_and_translated_exactly() {
    let mut assignments = Vec::with_capacity(64);
    let mut brokers = Vec::with_capacity(64);
    brokers.extend([7, 3]);
    assignments.push(CreateTopicReplicaAssignment::new(0, brokers));
    assignments.push(CreateTopicReplicaAssignment::new(1, vec![3, 9]));
    let request = CreateTopicsRequest::new(vec![
        CreateTopic::with_replica_assignments("orders", assignments, None).with_config(
            CreateTopicConfig::new("cleanup.policy", Some("compact".to_owned())),
        ),
    ]);
    assert!(!request.storage_is_canonical());

    let canonical = request.canonicalize();
    assert!(canonical.storage_is_canonical());
    assert!(canonical.retained_charge().is_some_and(|charge| charge > 0));
    let plan = canonical
        .into_plan()
        .unwrap_or_else(|error| panic!("valid manual engine request: {error}"));
    let CreateTopicPlacement::Manual { assignments, .. } = plan.topics()[0].placement() else {
        panic!("manual engine request must remain explicit");
    };
    assert_eq!(assignments[0].partition_index(), 0);
    assert_eq!(assignments[0].broker_ids(), &[7, 3]);
    assert_eq!(assignments[1].broker_ids(), &[3, 9]);
}
