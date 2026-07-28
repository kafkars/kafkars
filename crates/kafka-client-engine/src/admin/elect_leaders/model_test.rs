//! Engine request canonicalization and deterministic-plan translation.

use kafka_client_core::LeaderElectionType as CoreType;

use super::{ElectLeadersRequest, LeaderElectionTarget, LeaderElectionType};

#[test]
fn engine_request_preserves_type_and_target_order() {
    let plan = ElectLeadersRequest::new(
        LeaderElectionType::Unclean,
        vec![
            LeaderElectionTarget::new("orders", 2),
            LeaderElectionTarget::new("audit", 0),
        ],
    )
    .canonicalize()
    .into_plan()
    .unwrap_or_else(|error| panic!("valid request: {error}"));

    assert_eq!(plan.election_type(), CoreType::Unclean);
    assert_eq!(plan.targets()[0].topic(), "orders");
    assert_eq!(plan.targets()[0].partition(), 2);
    assert_eq!(plan.targets()[1].topic(), "audit");
}

#[test]
fn preparation_charge_accounts_for_target_storage() {
    let one = ElectLeadersRequest::new(
        LeaderElectionType::Preferred,
        vec![LeaderElectionTarget::new("a", 0)],
    );
    let two = ElectLeadersRequest::new(
        LeaderElectionType::Preferred,
        vec![
            LeaderElectionTarget::new("a", 0),
            LeaderElectionTarget::new("orders", 1),
        ],
    );
    assert!(two.preparation_charge() > one.preparation_charge());
}
