//! Accepted-call ownership scenarios for selected partition elections.

use std::time::{Duration, Instant};

use kafka_client_core::{
    Deadline, ElectLeadersPlan, LeaderElectionTarget, LeaderElectionType as CoreElectionType,
    Moment,
};

use crate::{
    EngineConfig,
    clock::OperationDeadline,
    driver::{DriverOwner, ElectLeadersCall},
};

use super::{
    ElectLeadersDeliveryStatus, ElectLeadersFailureKind, ElectLeadersHost, ElectLeadersHostError,
    ElectLeadersOutcome, ElectLeadersTurn,
};

#[test]
fn completion_fault_retains_call_until_post_driver_transport_settlement() {
    let (notifier, ports) = crate::admin::test_support::completion_owner();
    let mut host = ElectLeadersHost::new(ports.elect_leaders);
    let deadline = OperationDeadline::from_parts_for_test(
        Deadline::from_tick(10),
        Instant::now() + Duration::from_secs(1),
    );
    let plan = ElectLeadersPlan::new(
        CoreElectionType::Preferred,
        vec![LeaderElectionTarget::new("orders".to_owned(), 0)],
    )
    .unwrap_or_else(|error| panic!("plan: {error}"));
    let admission = host
        .try_admit(Moment::from_tick(1), deadline, plan)
        .unwrap_or_else(|error| panic!("admission: {error:?}"));
    let ElectLeadersTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("submission turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, submitted_deadline, submitted_plan, request_scratch_limit) =
        submission.into_parts();
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call = ElectLeadersCall::submit(
        &driver,
        &submitted_plan,
        request_scratch_limit,
        submitted_deadline,
        Moment::from_tick(2),
    )
    .unwrap_or_else(|error| panic!("accepted call: {error}"));
    host.accept_call(operation_id, call)
        .unwrap_or_else(|error| panic!("host acceptance: {error}"));
    drop(driver);

    assert!(matches!(
        host.turn(Moment::from_tick(3)),
        Err(ElectLeadersHostError::CallCompletion)
    ));
    assert_eq!(host.unsettled(), 1);
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
