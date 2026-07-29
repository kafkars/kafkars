//! Voter-addition plan validation scenarios.

use super::{
    ADD_RAFT_VOTER_MAX_LISTENERS, ADD_RAFT_VOTER_MAX_REQUEST_TEXT_BYTES,
    ADD_RAFT_VOTER_MAX_TEXT_BYTES, AddRaftVoterEndpoint, AddRaftVoterPlan, AddRaftVoterPlanError,
};

#[test]
fn plan_preserves_identity_optional_cluster_and_listener_order() {
    let plan = AddRaftVoterPlan::new(
        Some("cluster-a".to_owned()),
        7,
        [9; 16],
        vec![
            endpoint("CONTROLLER", "node-a", 9093),
            endpoint("INTERNAL", "node-a", 9094),
        ],
    )
    .unwrap_or_else(|error| panic!("plan: {error}"));

    assert_eq!(plan.cluster_id(), Some("cluster-a"));
    assert_eq!(plan.voter_id(), 7);
    assert_eq!(plan.voter_directory_id(), [9; 16]);
    assert_eq!(plan.listeners()[0].name(), "CONTROLLER");
    assert_eq!(plan.listeners()[1].port(), 9094);
    assert!(plan.ack_when_committed());
    assert_eq!(plan.minimum_api_version(), 0);
    let (cluster, voter, directory, listeners, ack_when_committed) = plan.into_parts();
    assert_eq!(cluster.as_deref(), Some("cluster-a"));
    assert_eq!(voter, 7);
    assert_eq!(directory, [9; 16]);
    assert!(ack_when_committed);
    assert_eq!(
        listeners[0].clone().into_parts(),
        ("CONTROLLER".to_owned(), "node-a".to_owned(), 9093)
    );
}

#[test]
fn explicit_early_acknowledgment_requires_v1_and_survives_ownership_transfer() {
    let plan = AddRaftVoterPlan::new(None, 7, [9; 16], vec![valid_endpoint()])
        .unwrap_or_else(|error| panic!("plan: {error}"))
        .with_ack_when_committed(false);

    assert!(!plan.ack_when_committed());
    assert_eq!(plan.minimum_api_version(), 1);
    let (_, _, _, listeners, ack_when_committed) = plan.into_parts();
    assert_eq!(listeners, vec![valid_endpoint()]);
    assert!(!ack_when_committed);
}

#[test]
fn invalid_cluster_and_voter_identity_are_rejected() {
    for (plan, expected) in [
        (
            AddRaftVoterPlan::new(Some(String::new()), 0, [1; 16], vec![valid_endpoint()]),
            AddRaftVoterPlanError::EmptyClusterId,
        ),
        (
            AddRaftVoterPlan::new(
                Some("x".repeat(ADD_RAFT_VOTER_MAX_TEXT_BYTES + 1)),
                0,
                [1; 16],
                vec![valid_endpoint()],
            ),
            AddRaftVoterPlanError::ClusterIdTooLong,
        ),
        (
            AddRaftVoterPlan::new(None, -1, [1; 16], vec![valid_endpoint()]),
            AddRaftVoterPlanError::NegativeVoterId,
        ),
        (
            AddRaftVoterPlan::new(None, 0, [0; 16], vec![valid_endpoint()]),
            AddRaftVoterPlanError::ZeroVoterDirectoryId,
        ),
    ] {
        assert_eq!(plan, Err(expected));
    }
}

#[test]
fn listener_count_shape_and_unique_names_are_bounded() {
    assert_eq!(
        AddRaftVoterPlan::new(None, 0, [1; 16], Vec::new()),
        Err(AddRaftVoterPlanError::EmptyListeners)
    );
    assert_eq!(
        AddRaftVoterPlan::new(
            None,
            0,
            [1; 16],
            (0..=ADD_RAFT_VOTER_MAX_LISTENERS)
                .map(|index| endpoint(&format!("L{index}"), "node", 9093))
                .collect(),
        ),
        Err(AddRaftVoterPlanError::TooManyListeners)
    );
    assert_eq!(
        AddRaftVoterPlan::new(
            None,
            0,
            [1; 16],
            vec![
                endpoint("CONTROLLER", "a", 9093),
                endpoint("CONTROLLER", "b", 9094),
            ],
        ),
        Err(AddRaftVoterPlanError::DuplicateListenerName)
    );
}

#[test]
fn every_invalid_listener_scalar_is_rejected() {
    for (listener, expected) in [
        (
            endpoint("", "node", 1),
            AddRaftVoterPlanError::EmptyListenerName,
        ),
        (
            endpoint(&"x".repeat(ADD_RAFT_VOTER_MAX_TEXT_BYTES + 1), "node", 1),
            AddRaftVoterPlanError::ListenerNameTooLong,
        ),
        (
            endpoint("CONTROLLER", "", 1),
            AddRaftVoterPlanError::EmptyListenerHost,
        ),
        (
            endpoint(
                "CONTROLLER",
                &"x".repeat(ADD_RAFT_VOTER_MAX_TEXT_BYTES + 1),
                1,
            ),
            AddRaftVoterPlanError::ListenerHostTooLong,
        ),
        (
            endpoint("CONTROLLER", "node", 0),
            AddRaftVoterPlanError::ZeroListenerPort,
        ),
    ] {
        assert_eq!(
            AddRaftVoterPlan::new(None, 0, [1; 16], vec![listener]),
            Err(expected)
        );
    }
}

#[test]
fn aggregate_request_text_is_bounded_independently_of_scalar_bounds() {
    let scalar = "x".repeat(ADD_RAFT_VOTER_MAX_TEXT_BYTES);
    let listeners = (0..5)
        .map(|index| endpoint(&format!("L{index}{scalar}"), &scalar, 1))
        .collect();
    assert_eq!(
        AddRaftVoterPlan::new(None, 0, [1; 16], listeners),
        Err(AddRaftVoterPlanError::ListenerNameTooLong)
    );

    let value = "x".repeat(ADD_RAFT_VOTER_MAX_TEXT_BYTES);
    let listeners = (0..5)
        .map(|index| endpoint(&format!("L{index}"), &value, 1))
        .collect();
    assert_eq!(
        AddRaftVoterPlan::new(
            Some("c".repeat(ADD_RAFT_VOTER_MAX_TEXT_BYTES)),
            0,
            [1; 16],
            listeners,
        ),
        Ok(AddRaftVoterPlan::new(
            Some("c".repeat(ADD_RAFT_VOTER_MAX_TEXT_BYTES)),
            0,
            [1; 16],
            (0..5)
                .map(|index| endpoint(&format!("L{index}"), &value, 1))
                .collect(),
        )
        .unwrap_or_else(|error| panic!("under aggregate limit: {error}")))
    );

    let listeners = (0..8)
        .map(|index| endpoint(&format!("L{index}"), &value, 1))
        .collect();
    assert_eq!(
        AddRaftVoterPlan::new(
            Some("c".repeat(ADD_RAFT_VOTER_MAX_TEXT_BYTES)),
            0,
            [1; 16],
            listeners,
        ),
        Err(AddRaftVoterPlanError::RequestTextBytesExceeded)
    );
    assert_eq!(ADD_RAFT_VOTER_MAX_REQUEST_TEXT_BYTES, 256 * 1024);
}

fn valid_endpoint() -> AddRaftVoterEndpoint {
    endpoint("CONTROLLER", "node", 9093)
}

fn endpoint(name: &str, host: &str, port: u16) -> AddRaftVoterEndpoint {
    AddRaftVoterEndpoint::new(name.to_owned(), host.to_owned(), port)
}
