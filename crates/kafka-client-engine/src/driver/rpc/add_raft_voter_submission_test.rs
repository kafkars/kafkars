//! AnyBroker route, original deadline, lane, and v0-v1 submission evidence.

use std::time::{Duration, Instant};

use kafka_client_core::{AddRaftVoterEndpoint, AddRaftVoterPlan};
use kafka_driver::{ApiVersion, Route, TrafficClass};

use super::add_raft_voter_submission::{add_raft_voter_options, add_raft_voter_route};

#[test]
fn mutation_uses_any_broker_and_preserves_original_deadline() {
    let deadline = Instant::now() + Duration::from_secs(5);
    let plan = plan();
    let options = add_raft_voter_options(&plan, deadline);

    assert_eq!(add_raft_voter_route(), Route::AnyBroker);
    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.minimum_version(), Some(ApiVersion::new(0)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(1)));
}

#[test]
fn local_write_acknowledgement_raises_the_floor_to_v1() {
    let deadline = Instant::now() + Duration::from_secs(5);
    let plan = plan().with_ack_when_committed(false);
    let options = add_raft_voter_options(&plan, deadline);

    assert_eq!(options.minimum_version(), Some(ApiVersion::new(1)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(1)));
}

fn plan() -> AddRaftVoterPlan {
    AddRaftVoterPlan::new(
        Some("cluster-a".to_owned()),
        7,
        [9; 16],
        vec![AddRaftVoterEndpoint::new(
            "CONTROLLER".to_owned(),
            "controller-a".to_owned(),
            9093,
        )],
    )
    .unwrap_or_else(|error| panic!("valid plan: {error}"))
}
