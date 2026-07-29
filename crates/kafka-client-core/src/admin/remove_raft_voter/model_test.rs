//! Voter-removal plan validation scenarios.

use super::{
    REMOVE_RAFT_VOTER_MAX_CLUSTER_ID_BYTES, RemoveRaftVoterPlan, RemoveRaftVoterPlanError,
};

#[test]
fn plan_preserves_optional_cluster_and_complete_voter_identity() {
    let plan = RemoveRaftVoterPlan::new(Some("cluster-a".to_owned()), 7, [9; 16])
        .unwrap_or_else(|error| panic!("plan: {error}"));

    assert_eq!(plan.cluster_id(), Some("cluster-a"));
    assert_eq!(plan.voter_id(), 7);
    assert_eq!(plan.voter_directory_id(), [9; 16]);
    assert_eq!(
        plan.into_parts(),
        (Some("cluster-a".to_owned()), 7, [9; 16])
    );

    let absent = RemoveRaftVoterPlan::new(None, 0, [1; 16])
        .unwrap_or_else(|error| panic!("absent cluster: {error}"));
    assert_eq!(absent.cluster_id(), None);
}

#[test]
fn invalid_cluster_and_voter_identity_are_rejected() {
    for (plan, expected) in [
        (
            RemoveRaftVoterPlan::new(Some(String::new()), 0, [1; 16]),
            RemoveRaftVoterPlanError::EmptyClusterId,
        ),
        (
            RemoveRaftVoterPlan::new(
                Some("x".repeat(REMOVE_RAFT_VOTER_MAX_CLUSTER_ID_BYTES + 1)),
                0,
                [1; 16],
            ),
            RemoveRaftVoterPlanError::ClusterIdTooLong,
        ),
        (
            RemoveRaftVoterPlan::new(None, -1, [1; 16]),
            RemoveRaftVoterPlanError::NegativeVoterId,
        ),
        (
            RemoveRaftVoterPlan::new(None, 0, [0; 16]),
            RemoveRaftVoterPlanError::ZeroVoterDirectoryId,
        ),
    ] {
        assert_eq!(plan, Err(expected));
    }
}
