//! Controller routing and exact election version bounds.

use std::time::{Duration, Instant};

use kafka_client_core::{
    Deadline, ElectLeadersPlan, LeaderElectionTarget, LeaderElectionType, Moment,
};
use kafka_driver::{ApiVersion, CompletionError, TrafficClass};

use crate::{EngineConfig, clock::OperationDeadline, driver::DriverOwner};

use super::{ElectLeadersCall, elect_leaders_submission::elect_leaders_options};

#[test]
fn election_uses_original_deadline_interactive_lane_and_type_floor() {
    let deadline = Instant::now() + Duration::from_secs(3);
    let options = elect_leaders_options(LeaderElectionType::Preferred, deadline);
    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.minimum_version(), Some(ApiVersion::new(0)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(2)));
    let unclean = elect_leaders_options(LeaderElectionType::Unclean, deadline);
    assert_eq!(unclean.minimum_version(), Some(ApiVersion::new(1)));
}

#[test]
fn completion_fault_remains_recoverable_after_driver_shutdown() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let plan = ElectLeadersPlan::new(
        LeaderElectionType::Preferred,
        vec![LeaderElectionTarget::new("orders".to_owned(), 0)],
    )
    .unwrap_or_else(|error| panic!("plan: {error}"));
    let deadline = OperationDeadline::from_parts_for_test(
        Deadline::from_tick(10),
        Instant::now() + Duration::from_secs(1),
    );
    let mut call = ElectLeadersCall::submit(&driver, &plan, 4_096, deadline, Moment::from_tick(1))
        .unwrap_or_else(|error| panic!("accepted call: {error}"));
    drop(driver);

    assert!(matches!(
        call.try_terminal(),
        Some(Err(CompletionError::Closed))
    ));
    call.recover_after_driver_shutdown()
        .unwrap_or_else(|| panic!("completion fault must retain accepted call ownership"))
        .seal();
}
