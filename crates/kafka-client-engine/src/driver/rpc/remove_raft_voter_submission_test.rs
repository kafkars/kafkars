//! AnyBroker route, original deadline, lane, and exact-v0 submission evidence.

use std::time::{Duration, Instant};

use kafka_driver::{ApiVersion, Route, TrafficClass};

use super::remove_raft_voter_submission::{remove_raft_voter_options, remove_raft_voter_route};

#[test]
fn mutation_uses_any_broker_and_preserves_original_deadline() {
    let deadline = Instant::now() + Duration::from_secs(5);
    let options = remove_raft_voter_options(deadline);

    assert_eq!(remove_raft_voter_route(), Route::AnyBroker);
    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.minimum_version(), Some(ApiVersion::new(0)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(0)));
}
