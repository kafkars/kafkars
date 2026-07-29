//! Accepted-call completion and driver-shutdown ownership scenarios.

use std::time::{Duration, Instant};

use kafka_client_core::{DeleteShareGroupOffsetsMachineError, DeleteShareGroupOffsetsPlan, Moment};

use crate::{
    EngineConfig,
    clock::OperationDeadline,
    driver::{DeleteShareGroupOffsetsCall, DriverOwner, RecoveredDeleteShareGroupOffsetsCall},
};

use super::super::{
    DeleteShareGroupOffsetsDeliveryStatus, DeleteShareGroupOffsetsFailureKind,
    DeleteShareGroupOffsetsHostError, DeleteShareGroupOffsetsOutcome, DeleteShareGroupOffsetsTurn,
};
use super::{DeleteShareGroupOffsetsAdmission, DeleteShareGroupOffsetsHost};

#[test]
fn handed_off_without_a_returned_call_cannot_forge_recovery_evidence() {
    let (mut host, mut notifier, admission) = admitted_host();
    let DeleteShareGroupOffsetsTurn::Submit(_submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("handoff turn: {error}"))
    else {
        panic!("submission expected");
    };

    assert!(matches!(
        host.recover_after_driver_shutdown(),
        Err(DeleteShareGroupOffsetsHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(&mut notifier);
}

#[test]
fn recovered_call_remains_retained_when_core_rejects_terminal_fact() {
    let (mut host, mut notifier, admission) = admitted_host();
    host.operations[0].recovered_call = Some(RecoveredDeleteShareGroupOffsetsCall::for_test());

    assert!(matches!(
        host.settle_recovered_transport(0),
        Err(DeleteShareGroupOffsetsHostError::Machine(
            DeleteShareGroupOffsetsMachineError::InvalidState
        ))
    ));
    assert!(host.operations[0].recovered_call.is_some());

    drop((admission, host));
    stop_notifier(&mut notifier);
}

#[test]
fn completion_fault_retains_call_until_post_driver_recovery() {
    let (mut host, mut notifier, admission) = admitted_host();
    let DeleteShareGroupOffsetsTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("submission turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, deadline, plan, _result_limit) = submission.into_parts();
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call = DeleteShareGroupOffsetsCall::submit(&driver, &plan, deadline.transport())
        .unwrap_or_else(|error| panic!("accepted call: {error}"));
    host.accept_call(operation_id, call)
        .unwrap_or_else(|error| panic!("host acceptance: {error}"));
    drop(driver);

    assert!(matches!(
        host.turn(Moment::from_tick(3)),
        Err(DeleteShareGroupOffsetsHostError::CallCompletion)
    ));
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("post-driver recovery: {error}"));
    let DeleteShareGroupOffsetsOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("recovery observation: {error}"))
    else {
        panic!("recovery failure expected");
    };
    assert_eq!(
        (failure.kind(), failure.delivery()),
        (
            DeleteShareGroupOffsetsFailureKind::Transport,
            DeleteShareGroupOffsetsDeliveryStatus::PossiblySent,
        )
    );

    drop(host);
    stop_notifier(&mut notifier);
}

fn admitted_host() -> (
    DeleteShareGroupOffsetsHost,
    crate::admin::AdminCompletionNotifier,
    DeleteShareGroupOffsetsAdmission,
) {
    let (notifier, ports) = crate::admin::AdminCompletionNotifier::start()
        .unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = DeleteShareGroupOffsetsHost::new(ports.delete_share_group_offsets);
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(20), plan())
        .unwrap_or_else(|error| panic!("admit API 92: {error:?}"));
    (host, notifier, admission)
}

fn plan() -> DeleteShareGroupOffsetsPlan {
    DeleteShareGroupOffsetsPlan::new(
        "payments-share".to_owned(),
        vec!["orders".to_owned(), "audit".to_owned()],
    )
    .unwrap_or_else(|error| panic!("valid API-92 plan: {error}"))
}

fn deadline(tick: u64) -> OperationDeadline {
    OperationDeadline::from_parts_for_test(
        kafka_client_core::Deadline::from_tick(tick),
        Instant::now() + Duration::from_secs(1),
    )
}

fn stop_notifier(notifier: &mut crate::admin::AdminCompletionNotifier) {
    notifier
        .stop()
        .unwrap_or_else(|error| panic!("stop notifier: {error}"))
        .join_off_notifier()
        .unwrap_or_else(|_| panic!("join notifier"));
}
