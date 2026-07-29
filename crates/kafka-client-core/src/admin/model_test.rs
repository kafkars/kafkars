//! Scenarios for validated ordered `CreateTopics` request facts.

use super::{
    CREATE_TOPICS_MAX_MANUAL_PARTITIONS_PER_TOPIC, CREATE_TOPICS_MAX_REPLICAS_PER_PARTITION,
    CreateTopicConfig, CreateTopicPlacement, CreateTopicReplicaAssignment,
    CreateTopicSpecification, CreateTopicsPlan, CreateTopicsPlanError,
};

fn topic(name: &str) -> CreateTopicSpecification {
    CreateTopicSpecification::new(
        name,
        3,
        2,
        vec![
            CreateTopicConfig::new("cleanup.policy", Some("compact".to_owned())),
            CreateTopicConfig::new("retention.ms", None),
        ],
    )
}

#[test]
fn plan_preserves_topic_and_nullable_config_order() {
    let result = CreateTopicsPlan::new(vec![topic("orders"), topic("audit")], true);
    assert!(result.is_ok());
    let Ok(plan) = result else {
        return;
    };

    assert!(plan.validate_only());
    assert_eq!(plan.topics()[0].name(), "orders");
    assert_eq!(plan.topics()[1].name(), "audit");
    assert_eq!(plan.topics()[0].configs()[0].name(), "cleanup.policy");
    assert_eq!(plan.topics()[0].configs()[0].value(), Some("compact"));
    assert_eq!(plan.topics()[0].configs()[1].value(), None);
}

#[test]
fn plan_rejects_empty_duplicate_and_invalid_topic_facts() {
    assert_eq!(
        CreateTopicsPlan::new(Vec::new(), false),
        Err(CreateTopicsPlanError::EmptyBatch)
    );
    assert_eq!(
        CreateTopicsPlan::new(vec![topic("orders"), topic("orders")], false),
        Err(CreateTopicsPlanError::DuplicateTopic)
    );
    assert_eq!(
        CreateTopicsPlan::new(
            vec![CreateTopicSpecification::new("orders", 0, 1, Vec::new())],
            false,
        ),
        Err(CreateTopicsPlanError::InvalidPartitionCount)
    );
    assert_eq!(
        CreateTopicsPlan::new(
            vec![CreateTopicSpecification::new("orders", 1, 0, Vec::new())],
            false,
        ),
        Err(CreateTopicsPlanError::InvalidReplicationFactor)
    );
}

#[test]
fn manual_plan_preserves_contiguous_partition_and_replica_order() {
    let plan = CreateTopicsPlan::new(
        vec![CreateTopicSpecification::manual(
            "orders",
            vec![
                CreateTopicReplicaAssignment::new(0, vec![7, 3]),
                CreateTopicReplicaAssignment::new(1, vec![3, 9]),
            ],
            None,
            vec![CreateTopicConfig::new(
                "cleanup.policy",
                Some("compact".to_owned()),
            )],
        )],
        false,
    )
    .unwrap_or_else(|error| panic!("valid manual placement: {error}"));

    let CreateTopicPlacement::Manual { assignments, .. } = plan.topics()[0].placement() else {
        panic!("manual placement must remain explicit");
    };
    assert_eq!(plan.topics()[0].partitions(), -1);
    assert_eq!(plan.topics()[0].replication_factor(), -1);
    assert_eq!(assignments[0].partition_index(), 0);
    assert_eq!(assignments[0].broker_ids(), &[7, 3]);
    assert_eq!(assignments[1].partition_index(), 1);
    assert_eq!(assignments[1].broker_ids(), &[3, 9]);
}

#[test]
fn manual_plan_rejects_mixed_or_malformed_placement() {
    let cases = [
        (
            Vec::new(),
            None,
            CreateTopicsPlanError::EmptyManualAssignments,
        ),
        (
            vec![CreateTopicReplicaAssignment::new(1, vec![1])],
            None,
            CreateTopicsPlanError::NonContiguousManualPartitions,
        ),
        (
            vec![CreateTopicReplicaAssignment::new(0, Vec::new())],
            None,
            CreateTopicsPlanError::EmptyManualReplicaSet,
        ),
        (
            vec![CreateTopicReplicaAssignment::new(0, vec![-1])],
            None,
            CreateTopicsPlanError::NegativeBrokerId,
        ),
        (
            vec![CreateTopicReplicaAssignment::new(0, vec![1, 1])],
            None,
            CreateTopicsPlanError::DuplicateBrokerId,
        ),
        (
            vec![CreateTopicReplicaAssignment::new(0, vec![1])],
            Some(3),
            CreateTopicsPlanError::MixedReplicaPlacement,
        ),
    ];
    for (assignments, conflicting_replication_factor, expected) in cases {
        assert_eq!(
            CreateTopicsPlan::new(
                vec![CreateTopicSpecification::manual(
                    "orders",
                    assignments,
                    conflicting_replication_factor,
                    Vec::new(),
                )],
                false,
            ),
            Err(expected),
        );
    }
}

#[test]
fn manual_plan_enforces_explicit_count_bounds() {
    let too_many_partitions = (0..=CREATE_TOPICS_MAX_MANUAL_PARTITIONS_PER_TOPIC)
        .map(|partition| CreateTopicReplicaAssignment::new(partition as i32, vec![1]))
        .collect();
    assert_eq!(
        CreateTopicsPlan::new(
            vec![CreateTopicSpecification::manual(
                "orders",
                too_many_partitions,
                None,
                Vec::new(),
            )],
            false,
        ),
        Err(CreateTopicsPlanError::TooManyManualPartitions),
    );

    assert_eq!(
        CreateTopicsPlan::new(
            vec![CreateTopicSpecification::manual(
                "orders",
                vec![CreateTopicReplicaAssignment::new(
                    0,
                    vec![1; CREATE_TOPICS_MAX_REPLICAS_PER_PARTITION + 1],
                )],
                None,
                Vec::new(),
            )],
            false,
        ),
        Err(CreateTopicsPlanError::TooManyReplicas),
    );
}
