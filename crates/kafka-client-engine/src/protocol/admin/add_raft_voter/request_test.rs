//! Exact identity, endpoint, timeout, and acknowledgement request evidence.

use kafka_client_core::{AddRaftVoterEndpoint, AddRaftVoterPlan};

use super::{AddRaftVoterRequestFailure, add_raft_voter_request};

#[test]
fn request_preserves_plan_and_default_committed_acknowledgement() {
    let plan = plan();
    let request = add_raft_voter_request(&plan, 321).expect("valid request");

    assert_eq!(request.cluster_id.as_deref(), Some("cluster-a"));
    assert_eq!(request.timeout_ms, 321);
    assert_eq!(request.voter_id, 7);
    assert_eq!(request.voter_directory_id.to_bytes(), [9; 16]);
    assert!(request.ack_when_committed);
    assert_eq!(request.listeners.len(), 2);
    assert_eq!(request.listeners[0].name.as_str(), "CONTROLLER");
    assert_eq!(request.listeners[0].host.as_str(), "controller-a");
    assert_eq!(request.listeners[0].port, 9093);
    assert_eq!(request.listeners[1].name.as_str(), "CONTROLLER_SSL");
    assert_eq!(request.listeners[1].host.as_str(), "controller-b");
    assert_eq!(request.listeners[1].port, 9094);
    assert!(request.unknown_tagged_fields.is_empty());
    assert!(
        request
            .listeners
            .iter()
            .all(|listener| listener.unknown_tagged_fields.is_empty())
    );
}

#[test]
fn request_preserves_explicit_local_write_acknowledgement() {
    let plan = plan().with_ack_when_committed(false);
    let request = add_raft_voter_request(&plan, 321).expect("valid local-write request");

    assert!(!request.ack_when_committed);
}

#[test]
fn request_preserves_absent_cluster_and_rejects_nonpositive_timeout() {
    let plan = AddRaftVoterPlan::new(
        None,
        1,
        [1; 16],
        vec![AddRaftVoterEndpoint::new(
            "CONTROLLER".to_owned(),
            "host".to_owned(),
            1,
        )],
    )
    .unwrap_or_else(|error| panic!("valid plan: {error}"));
    assert_eq!(
        add_raft_voter_request(&plan, 0),
        Err(AddRaftVoterRequestFailure::NonPositiveTimeout { actual: 0 })
    );
    let request = add_raft_voter_request(&plan, 1).expect("positive timeout");
    assert_eq!(request.cluster_id, None);
}

fn plan() -> AddRaftVoterPlan {
    AddRaftVoterPlan::new(
        Some("cluster-a".to_owned()),
        7,
        [9; 16],
        vec![
            AddRaftVoterEndpoint::new("CONTROLLER".to_owned(), "controller-a".to_owned(), 9093),
            AddRaftVoterEndpoint::new("CONTROLLER_SSL".to_owned(), "controller-b".to_owned(), 9094),
        ],
    )
    .unwrap_or_else(|error| panic!("valid plan: {error}"))
}
