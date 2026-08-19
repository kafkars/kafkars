//! Atomic all-partitions admission and deadline-retention scenarios.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use kafka_client_core::{
    Deadline, ElectLeadersPlan, LeaderElectionType as CoreElectionType, Moment,
};

use crate::{
    EngineConfig,
    clock::{MonotonicClock, OperationDeadline},
    driver::{DriverOwner, ElectLeadersCall},
};

use super::{
    ElectLeadersAdmissionErrorKind, ElectLeadersDeliveryStatus, ElectLeadersFailureKind,
    ElectLeadersHost, ElectLeadersHostError, ElectLeadersOutcome, ElectLeadersTurn,
};

#[test]
fn all_partitions_reserves_before_machine_and_retains_original_deadline() {
    let (notifier, ports) = crate::admin::test_support::completion_owner();
    let mut host = ElectLeadersHost::new(ports.elect_leaders);
    let clock = Arc::new(MonotonicClock::new());
    let capture = clock
        .capture_deadline_after(Duration::from_secs(5))
        .unwrap_or_else(|error| panic!("capture deadline: {error}"));
    let deadline = capture.operation_deadline();
    let admission = host
        .try_admit(
            capture.now(),
            deadline,
            ElectLeadersPlan::all(CoreElectionType::Preferred),
        )
        .unwrap_or_else(|error| panic!("admit all-partitions election: {error:?}"));

    assert!(admission.fault.is_none());
    assert_eq!(host.unsettled(), 1);
    assert!(matches!(
        host.try_admit(
            capture.now(),
            deadline,
            ElectLeadersPlan::all(CoreElectionType::Preferred),
        ),
        Err(ElectLeadersAdmissionErrorKind::RetainedBytes)
    ));

    let ElectLeadersTurn::Submit(submission) = host
        .turn(Moment::from_tick(capture.now().tick()))
        .unwrap_or_else(|error| panic!("take election submission: {error}"))
    else {
        panic!("expected election submission");
    };
    let (operation_id, submitted_deadline, plan, scratch_limit, result_limit) =
        submission.into_parts();
    assert_eq!(submitted_deadline, deadline);
    assert!(plan.selection().selected_targets().is_none());
    assert!(scratch_limit > 0);
    assert!(result_limit > 0);

    host.reject_handoff(operation_id, plan, scratch_limit, result_limit)
        .unwrap_or_else(|error| panic!("settle rejected handoff: {error}"));
    drop((admission, host));
    crate::admin::test_support::stop_notifier(notifier);
}

#[test]
fn completion_fault_retains_call_until_post_driver_transport_settlement() {
    let (notifier, ports) = crate::admin::test_support::completion_owner();
    let mut host = ElectLeadersHost::new(ports.elect_leaders);
    let deadline = OperationDeadline::from_parts_for_test(
        Deadline::from_tick(10),
        Instant::now() + Duration::from_secs(1),
    );
    let admission = host
        .try_admit(
            Moment::from_tick(1),
            deadline,
            ElectLeadersPlan::all(CoreElectionType::Preferred),
        )
        .unwrap_or_else(|error| panic!("admission: {error:?}"));
    let ElectLeadersTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("submission turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, submitted_deadline, submitted_plan, request_scratch_limit, result_limit) =
        submission.into_parts();
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call = ElectLeadersCall::submit(
        &driver,
        submitted_plan.clone(),
        request_scratch_limit,
        result_limit,
        submitted_deadline,
        Moment::from_tick(2),
    )
    .unwrap_or_else(|error| panic!("accepted call: {error}"));
    assert!(call.matches_correlation(&submitted_plan, request_scratch_limit, result_limit));
    host.accept_call(operation_id, call)
        .unwrap_or_else(|error| panic!("host acceptance: {error}"));
    drop(driver);

    assert!(matches!(
        host.turn(Moment::from_tick(3)),
        Err(ElectLeadersHostError::CallCompletion)
    ));
    assert_eq!(
        host.unsettled(),
        1,
        "completion failure must not publish while accepted call evidence remains"
    );
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("post-driver recovery: {error}"));
    assert_eq!(host.unsettled(), 0);
    let outcome = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("recovery observation: {error}"));
    let ElectLeadersOutcome::Failed(failure) = outcome else {
        panic!("recovery failure expected");
    };
    let (kind, delivery) = failure.into_parts();
    assert_eq!(kind, ElectLeadersFailureKind::Transport);
    assert_eq!(delivery, ElectLeadersDeliveryStatus::PossiblySent);

    drop(host);
    crate::admin::test_support::stop_notifier(notifier);
}
