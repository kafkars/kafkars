//! Exact identity and empty-extension request evidence.

use kafka_client_core::RemoveRaftVoterPlan;

use super::remove_raft_voter_request;

#[test]
fn request_preserves_identity_without_a_kafka_side_timeout() {
    let plan = RemoveRaftVoterPlan::new(Some("cluster-a".to_owned()), 7, [9; 16])
        .unwrap_or_else(|error| panic!("valid plan: {error}"));
    let request = remove_raft_voter_request(&plan);

    assert_eq!(request.cluster_id.as_deref(), Some("cluster-a"));
    assert_eq!(request.voter_id, 7);
    assert_eq!(request.voter_directory_id.to_bytes(), [9; 16]);
    assert!(request.unknown_tagged_fields.is_empty());
}

#[test]
fn request_preserves_absent_cluster_identity() {
    let plan = RemoveRaftVoterPlan::new(None, 1, [1; 16])
        .unwrap_or_else(|error| panic!("valid plan: {error}"));
    let request = remove_raft_voter_request(&plan);
    assert_eq!(request.cluster_id, None);
}
