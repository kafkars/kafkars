//! Deferred validation and exact request-to-core conversion evidence.

use super::{AddRaftVoterEndpoint, AddRaftVoterRequest, model::AddRaftVoterPlanFailure};

#[test]
fn request_preserves_cluster_voter_directory_and_listener_order() {
    let plan = request()
        .into_plan()
        .unwrap_or_else(|error| panic!("valid request: {error:?}"));
    assert_eq!(plan.cluster_id(), Some("cluster-a"));
    assert_eq!(plan.voter_id(), 7);
    assert_eq!(plan.voter_directory_id(), [9; 16]);
    assert_eq!(plan.listeners().len(), 2);
    assert_eq!(plan.listeners()[0].name(), "CONTROLLER");
    assert_eq!(plan.listeners()[1].name(), "CONTROLLER_SSL");
    assert!(plan.ack_when_committed());
}

#[test]
fn request_preserves_explicit_local_write_acknowledgement() {
    let plan = request()
        .ack_when_committed(false)
        .into_plan()
        .unwrap_or_else(|error| panic!("valid local-write request: {error:?}"));

    assert!(!plan.ack_when_committed());
}

#[test]
fn duplicate_listener_name_is_rejected_after_capture_boundary_conversion() {
    let duplicate = AddRaftVoterRequest::new(
        None,
        7,
        [9; 16],
        vec![
            AddRaftVoterEndpoint::new("CONTROLLER".to_owned(), "a".to_owned(), 1),
            AddRaftVoterEndpoint::new("CONTROLLER".to_owned(), "b".to_owned(), 2),
        ],
    );
    assert!(matches!(
        duplicate.into_plan(),
        Err(AddRaftVoterPlanFailure::Invalid)
    ));
}

fn request() -> AddRaftVoterRequest {
    AddRaftVoterRequest::new(
        Some("cluster-a".to_owned()),
        7,
        [9; 16],
        vec![
            AddRaftVoterEndpoint::new("CONTROLLER".to_owned(), "controller-a".to_owned(), 9093),
            AddRaftVoterEndpoint::new("CONTROLLER_SSL".to_owned(), "controller-b".to_owned(), 9094),
        ],
    )
}
