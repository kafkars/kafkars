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
    let plan = ElectLeadersPlan::all(LeaderElectionType::Preferred);
    let deadline = OperationDeadline::from_parts_for_test(
        Deadline::from_tick(10),
        Instant::now() + Duration::from_secs(1),
    );
    let mut call = ElectLeadersCall::submit(
        &driver,
        plan.clone(),
        4_096,
        8_192,
        deadline,
        Moment::from_tick(1),
    )
    .unwrap_or_else(|error| panic!("accepted call: {error}"));
    assert!(call.matches_correlation(&plan, 4_096, 8_192));
    drop(driver);

    assert!(matches!(
        call.try_terminal(),
        Some(Err(CompletionError::Closed))
    ));
    let recovered = call
        .recover_after_driver_shutdown()
        .unwrap_or_else(|| panic!("completion fault must retain accepted call ownership"));
    assert!(recovered.matches_correlation(&plan, 4_096, 8_192));
    recovered.seal();
}

#[test]
fn synchronous_deadline_rejection_returns_exact_attempt_correlation() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let plan = ElectLeadersPlan::selected(
        LeaderElectionType::Unclean,
        vec![
            LeaderElectionTarget::new("orders".to_owned(), 2),
            LeaderElectionTarget::new("payments".to_owned(), 7),
        ],
    )
    .unwrap_or_else(|error| panic!("selected election plan: {error}"));
    let deadline = OperationDeadline::from_parts_for_test(
        Deadline::from_tick(10),
        Instant::now() + Duration::from_secs(1),
    );
    let rejection = match ElectLeadersCall::submit(
        &driver,
        plan.clone(),
        4_096,
        8_192,
        deadline,
        Moment::from_tick(10),
    ) {
        Err(rejection) => rejection,
        Ok(_call) => panic!("elapsed call must be rejected before driver ownership"),
    };
    let (returned_plan, returned_scratch_limit, returned_result_limit) =
        rejection.into_correlation();
    assert_eq!(returned_plan, plan);
    assert_eq!(returned_scratch_limit, 4_096);
    assert_eq!(returned_result_limit, 8_192);
}
