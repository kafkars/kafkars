//! Focused response correlation and exact-error tests.

use kafka_client_core::{LeaderElectionResult, LeaderElectionType};
use kafka_wire::{
    ElectLeadersResponse,
    elect_leaders_response::{PartitionResult, ReplicaElectionResult},
};

use super::{LeaderElectionRef, ValidatedElectLeadersResponse, validate_elect_leaders_response};

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
        &targets,
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
fn unclean_response_rejects_v0() {
    let targets = [LeaderElectionRef::new("orders", 0)];
    assert!(
        validate_elect_leaders_response(
            LeaderElectionType::Unclean,
            &targets,
            &ElectLeadersResponse::default(),
            0,
            usize::MAX,
        )
        .is_err()
    );
}
