//! Exact-broker route and version-window evidence for `AlterReplicaLogDirs`.

use std::time::{Duration, Instant};

use kafka_client_core::AlterReplicaLogDirAssignment;
use kafka_driver::{CompletionError, Route, TrafficClass};

use crate::{EngineConfig, driver::DriverOwner};

use super::{
    AlterReplicaLogDirsCall, AlterReplicaLogDirsRawTerminal,
    alter_replica_log_dirs_submission::{
        alter_replica_log_dirs_options, alter_replica_log_dirs_route,
    },
};

#[test]
fn route_targets_the_requested_broker() {
    assert_eq!(
        alter_replica_log_dirs_route(17).expect("valid broker"),
        Route::AnyBroker
    );
    assert!(alter_replica_log_dirs_route(-1).is_err());
}

#[test]
fn options_preserve_deadline_lane_and_supported_versions() {
    let deadline = Instant::now() + Duration::from_secs(3);
    let options = alter_replica_log_dirs_options(deadline);

    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(
        options.minimum_version().map(|version| version.value()),
        Some(1)
    );
    assert_eq!(
        options.maximum_version().map(|version| version.value()),
        Some(2)
    );
}

#[test]
fn completion_fault_retains_the_accepted_call_for_recovery() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let assignments = vec![
        assignment(1, "orders", 0, "/data-a"),
        assignment(1, "orders", 2, "/data-c"),
    ];
    let request_scratch_limit = 4_096;
    let result_limit = 8_192;
    let mut call = AlterReplicaLogDirsCall::submit(
        &driver,
        1,
        assignments.clone(),
        request_scratch_limit,
        result_limit,
        Instant::now() + Duration::from_secs(1),
    )
    .unwrap_or_else(|_error| panic!("accepted call"));
    assert!(call.matches_evidence(1, &assignments, request_scratch_limit, result_limit));
    drop(driver);

    assert!(matches!(
        call.try_terminal(),
        Some(Err(CompletionError::Closed))
    ));
    let recovered = call
        .recover_after_driver_shutdown()
        .unwrap_or_else(|| panic!("completion fault must retain accepted call ownership"));
    assert!(recovered.matches_evidence(1, &assignments, request_scratch_limit, result_limit));
    recovered.seal();
}

#[test]
fn request_rejection_returns_exact_route_group_order_and_bounds() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let assignments = vec![
        assignment(7, "orders", 2, "/data-c"),
        assignment(7, "orders", 0, "/data-a"),
    ];
    let rejection = match AlterReplicaLogDirsCall::submit(
        &driver,
        7,
        assignments.clone(),
        0,
        8_192,
        Instant::now() + Duration::from_secs(1),
    ) {
        Ok(_call) => panic!("zero request scratch must reject"),
        Err(rejection) => rejection,
    };

    let (broker_id, returned, request_scratch_limit, result_limit) = rejection.into_evidence();
    assert_eq!(broker_id, 7);
    assert_eq!(returned, assignments);
    assert_eq!((request_scratch_limit, result_limit), (0, 8_192));
}

#[test]
fn raw_evidence_distinguishes_route_group_order_and_each_bound() {
    let assignments = vec![
        assignment(7, "orders", 2, "/data-c"),
        assignment(7, "orders", 0, "/data-a"),
    ];
    let reversed = assignments.iter().cloned().rev().collect::<Vec<_>>();
    let raw = AlterReplicaLogDirsRawTerminal::for_test(7, assignments.clone(), 4_096, 8_192);

    assert!(raw.matches_evidence(7, &assignments, 4_096, 8_192));
    assert!(!raw.matches_evidence(8, &assignments, 4_096, 8_192));
    assert!(!raw.matches_evidence(7, &reversed, 4_096, 8_192));
    assert!(!raw.matches_evidence(7, &assignments, 4_095, 8_192));
    assert!(!raw.matches_evidence(7, &assignments, 4_096, 8_191));
}

fn assignment(
    broker_id: i32,
    topic: &str,
    partition: i32,
    log_dir: &str,
) -> AlterReplicaLogDirAssignment {
    AlterReplicaLogDirAssignment::new(broker_id, topic.to_owned(), partition, log_dir.to_owned())
}
