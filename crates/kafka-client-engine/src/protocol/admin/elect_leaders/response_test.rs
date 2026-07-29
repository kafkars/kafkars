//! Focused response correlation and exact-error tests.

use kafka_client_core::{LeaderElectionResult, LeaderElectionType};
use kafka_wire::{
    ElectLeadersResponse,
    elect_leaders_response::{PartitionResult, ReplicaElectionResult},
};

use super::{
    ElectLeadersProtocolFailure, ElectLeadersSelectionRef, LeaderElectionRef,
    ValidatedElectLeadersResponse, validate_elect_leaders_response,
};

#[test]
fn response_is_restored_to_caller_order() {
    let targets = [
        LeaderElectionRef::new("orders", 2),
        LeaderElectionRef::new("audit", 0),
    ];
    let mut accepted = PartitionResult::default();
    accepted.partition_id = 0;
    accepted.error_code = 0;
    accepted.error_message = None;
    let mut audit = ReplicaElectionResult::default();
    audit.topic = "audit".into();
    audit.partition_result = vec![accepted];

    let mut rejected = PartitionResult::default();
    rejected.partition_id = 2;
    rejected.error_code = 87;
    rejected.error_message = Some("preferred replica unavailable".into());
    let mut orders = ReplicaElectionResult::default();
    orders.topic = "orders".into();
    orders.partition_result = vec![rejected];

    let mut response = ElectLeadersResponse::default();
    response.throttle_time_ms = 7;
    response.replica_election_results = vec![audit, orders];

    let ValidatedElectLeadersResponse::Batch(batch) = validate_elect_leaders_response(
        LeaderElectionType::Preferred,
        ElectLeadersSelectionRef::Selected(&targets),
        &response,
        2,
        usize::MAX,
    )
    .unwrap_or_else(|error| panic!("valid response: {error:?}")) else {
        panic!("expected batch");
    };
    assert_eq!(batch.throttle_time_ms(), 7);
    assert_eq!(batch.outcomes()[0].topic(), "orders");
    let LeaderElectionResult::Failed(error) = batch.outcomes()[0].result() else {
        panic!("expected exact partition error");
    };
    assert_eq!(error.code(), 87);
    assert_eq!(batch.outcomes()[1].topic(), "audit");
    assert_eq!(batch.outcomes()[1].result(), &LeaderElectionResult::Elected);
}

#[test]
fn all_partitions_accepts_empty_and_sorts_any_bounded_returned_set() {
    let ValidatedElectLeadersResponse::Batch(empty) = validate_elect_leaders_response(
        LeaderElectionType::Preferred,
        ElectLeadersSelectionRef::AllPartitions,
        &ElectLeadersResponse::default(),
        2,
        usize::MAX,
    )
    .unwrap_or_else(|error| panic!("empty all-partitions response: {error:?}")) else {
        panic!("expected empty batch");
    };
    assert!(empty.outcomes().is_empty());

    let mut orders_two = PartitionResult::default();
    orders_two.partition_id = 2;
    orders_two.error_code = 87;
    orders_two.error_message = Some("not eligible".into());
    let mut orders_one = PartitionResult::default();
    orders_one.partition_id = 1;
    let mut orders = ReplicaElectionResult::default();
    orders.topic = "orders".into();
    orders.partition_result = vec![orders_two, orders_one];
    let mut audit_zero = PartitionResult::default();
    audit_zero.partition_id = 0;
    let mut audit = ReplicaElectionResult::default();
    audit.topic = "audit".into();
    audit.partition_result = vec![audit_zero];
    let mut response = ElectLeadersResponse::default();
    response.throttle_time_ms = 19;
    response.replica_election_results = vec![orders, audit];

    let ValidatedElectLeadersResponse::Batch(batch) = validate_elect_leaders_response(
        LeaderElectionType::Preferred,
        ElectLeadersSelectionRef::AllPartitions,
        &response,
        2,
        usize::MAX,
    )
    .unwrap_or_else(|error| panic!("all-partitions response: {error:?}")) else {
        panic!("expected batch");
    };
    assert_eq!(batch.throttle_time_ms(), 19);
    assert_eq!(
        batch
            .outcomes()
            .iter()
            .map(|outcome| (outcome.topic(), outcome.partition()))
            .collect::<Vec<_>>(),
        vec![("audit", 0), ("orders", 1), ("orders", 2)]
    );
    let LeaderElectionResult::Failed(error) = batch.outcomes()[2].result() else {
        panic!("exact partition error expected");
    };
    assert_eq!(error.code(), 87);
}

#[test]
fn all_partitions_rejects_malformed_or_duplicate_returned_identities() {
    let mut partition = PartitionResult::default();
    partition.partition_id = 0;
    let mut empty_topic = ReplicaElectionResult::default();
    empty_topic.partition_result = vec![partition.clone()];
    let mut response = ElectLeadersResponse::default();
    response.replica_election_results = vec![empty_topic];
    assert_eq!(
        validate_elect_leaders_response(
            LeaderElectionType::Preferred,
            ElectLeadersSelectionRef::AllPartitions,
            &response,
            2,
            usize::MAX,
        ),
        Err(ElectLeadersProtocolFailure::EmptyTopic)
    );

    let mut first = ReplicaElectionResult::default();
    first.topic = "orders".into();
    first.partition_result = vec![partition.clone()];
    let mut second = ReplicaElectionResult::default();
    second.topic = "orders".into();
    second.partition_result = vec![partition];
    response.replica_election_results = vec![first, second];
    assert_eq!(
        validate_elect_leaders_response(
            LeaderElectionType::Preferred,
            ElectLeadersSelectionRef::AllPartitions,
            &response,
            2,
            usize::MAX,
        ),
        Err(ElectLeadersProtocolFailure::DuplicateTopic)
    );

    let mut duplicate = PartitionResult::default();
    duplicate.partition_id = 0;
    let mut duplicate_partition = ReplicaElectionResult::default();
    duplicate_partition.topic = "orders".into();
    duplicate_partition.partition_result = vec![duplicate.clone(), duplicate];
    response.replica_election_results = vec![duplicate_partition];
    assert_eq!(
        validate_elect_leaders_response(
            LeaderElectionType::Preferred,
            ElectLeadersSelectionRef::AllPartitions,
            &response,
            2,
            usize::MAX,
        ),
        Err(ElectLeadersProtocolFailure::DuplicatePartition)
    );

    let mut negative = PartitionResult::default();
    negative.partition_id = -1;
    let mut negative_partition = ReplicaElectionResult::default();
    negative_partition.topic = "orders".into();
    negative_partition.partition_result = vec![negative];
    response.replica_election_results = vec![negative_partition];
    assert_eq!(
        validate_elect_leaders_response(
            LeaderElectionType::Preferred,
            ElectLeadersSelectionRef::AllPartitions,
            &response,
            2,
            usize::MAX,
        ),
        Err(ElectLeadersProtocolFailure::NegativePartition)
    );

    response.throttle_time_ms = -1;
    response.replica_election_results.clear();
    assert_eq!(
        validate_elect_leaders_response(
            LeaderElectionType::Preferred,
            ElectLeadersSelectionRef::AllPartitions,
            &response,
            2,
            usize::MAX,
        ),
        Err(ElectLeadersProtocolFailure::NegativeThrottleTime)
    );
}

#[test]
fn unclean_response_rejects_v0() {
    let targets = [LeaderElectionRef::new("orders", 0)];
    assert!(
        validate_elect_leaders_response(
            LeaderElectionType::Unclean,
            ElectLeadersSelectionRef::Selected(&targets),
            &ElectLeadersResponse::default(),
            0,
            usize::MAX,
        )
        .is_err()
    );
}
