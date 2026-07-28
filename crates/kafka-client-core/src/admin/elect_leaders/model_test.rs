//! Focused validation tests for explicit selected-partition elections.

use super::{ElectLeadersPlan, ElectLeadersPlanError, LeaderElectionTarget, LeaderElectionType};

#[test]
fn plan_preserves_type_and_caller_order() {
    let plan = ElectLeadersPlan::new(
        LeaderElectionType::Unclean,
        vec![
            LeaderElectionTarget::new("orders".into(), 2),
            LeaderElectionTarget::new("audit".into(), 0),
        ],
    )
    .expect("valid plan");

    assert_eq!(plan.election_type(), LeaderElectionType::Unclean);
    assert_eq!(plan.targets()[0].topic(), "orders");
    assert_eq!(plan.targets()[0].partition(), 2);
    assert_eq!(plan.targets()[1].topic(), "audit");
}

#[test]
fn plan_rejects_invalid_and_duplicate_targets() {
    for (targets, expected) in [
        (vec![], ElectLeadersPlanError::EmptyBatch),
        (
            vec![LeaderElectionTarget::new(String::new(), 0)],
            ElectLeadersPlanError::EmptyTopicName,
        ),
        (
            vec![LeaderElectionTarget::new("orders".into(), -1)],
            ElectLeadersPlanError::NegativePartition,
        ),
        (
            vec![
                LeaderElectionTarget::new("orders".into(), 1),
                LeaderElectionTarget::new("orders".into(), 1),
            ],
            ElectLeadersPlanError::DuplicateTopicPartition,
        ),
    ] {
        assert_eq!(
            ElectLeadersPlan::new(LeaderElectionType::Preferred, targets),
            Err(expected)
        );
    }
}
