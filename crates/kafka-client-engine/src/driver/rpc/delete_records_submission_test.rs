//! Admin `DeleteRecords` leader route and exact version-window scenarios.

use std::time::{Duration, Instant};

use kafka_client_core::DeleteRecordsTarget;
use kafka_driver::{ApiVersion, CompletionError, TrafficClass};

use crate::{EngineConfig, driver::DriverOwner};

use super::{DeleteRecordsCall, delete_records_submission::delete_records_options};

#[test]
fn options_preserve_deadline_lane_and_v0_through_v2_window() {
    let deadline = Instant::now() + Duration::from_secs(7);
    let options = delete_records_options(deadline);

    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.minimum_version(), Some(ApiVersion::new(0)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(2)));
}

#[test]
fn completion_fault_remains_recoverable_after_driver_shutdown() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let target = DeleteRecordsTarget::new("orders".to_owned(), 2, 91);
    let mut call = DeleteRecordsCall::submit(
        &driver,
        &target,
        1_000,
        Instant::now() + Duration::from_secs(1),
    )
    .unwrap_or_else(|_error| panic!("accepted call"));
    drop(driver);

    assert!(matches!(
        call.try_terminal(),
        Some(Err(CompletionError::Closed))
    ));
    call.recover_after_driver_shutdown()
        .unwrap_or_else(|| panic!("completion fault must retain accepted call ownership"))
        .seal();
}
