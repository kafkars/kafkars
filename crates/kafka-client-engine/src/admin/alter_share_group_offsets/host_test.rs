//! Admission, deadline, recovery, and retained-envelope scenarios for API 91.

use std::time::{Duration, Instant};

use kafka_client_core::{
    AlterShareGroupOffset, AlterShareGroupOffsetsMachineError, AlterShareGroupOffsetsPlan, Moment,
};

use crate::{
    EngineConfig,
    admin::AdminCompletionNotifier,
    clock::OperationDeadline,
    driver::{AlterShareGroupOffsetsCall, DriverOwner},
};

use super::{
    AlterShareGroupOffsetsAdmissionErrorKind, AlterShareGroupOffsetsDeliveryStatus,
    AlterShareGroupOffsetsFailureKind, AlterShareGroupOffsetsHost, AlterShareGroupOffsetsHostError,
    AlterShareGroupOffsetsOutcome, AlterShareGroupOffsetsTurn,
    host::{ALTER_SHARE_GROUP_OFFSETS_RESULT_BYTES, ALTER_SHARE_GROUP_OFFSETS_RETAINED_BYTES},
};

#[test]
fn admission_reserves_request_owner_and_complete_result_before_submission() {
    let (mut host, mut notifier) = host();
    let deadline = deadline(20);
    let admission = host
        .try_admit(Moment::from_tick(1), deadline, plan())
        .unwrap_or_else(|error| panic!("admit API 91: {error:?}"));

    assert!(admission.fault.is_none());
    assert_eq!(host.unsettled(), 1);
    assert!(host.retained_bytes_for_test() > ALTER_SHARE_GROUP_OFFSETS_RESULT_BYTES);
    assert!(host.retained_bytes_for_test() < ALTER_SHARE_GROUP_OFFSETS_RETAINED_BYTES);
    let AlterShareGroupOffsetsTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("submission turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, submitted_deadline, submitted_plan, result_limit) = submission.into_parts();
    assert_eq!(submitted_deadline, deadline);
    assert_eq!(submitted_plan, plan());
    assert_eq!(result_limit, ALTER_SHARE_GROUP_OFFSETS_RESULT_BYTES);
    assert_eq!(host.next_deadline(), None);

    drop(admission.observer);
    host.reject_handoff(operation_id)
        .unwrap_or_else(|error| panic!("reject untouched driver handoff: {error}"));
    drop(host);
    stop_notifier(&mut notifier);
}

#[test]
fn bounded_aggregate_reservation_eventually_rejects_before_machine_construction() {
    let (mut host, mut notifier) = host();
    let deadline = deadline(20);
    let mut accepted = Vec::new();
    loop {
        match host.try_admit(Moment::from_tick(1), deadline, plan()) {
            Ok(admission) => accepted.push(admission),
            Err(AlterShareGroupOffsetsAdmissionErrorKind::RetainedBytes) => break,
            Err(error) => panic!("unexpected admission failure: {error:?}"),
        }
    }

    assert!(!accepted.is_empty());
    assert!(host.retained_bytes_for_test() <= ALTER_SHARE_GROUP_OFFSETS_RETAINED_BYTES);
    drop(accepted);
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover admitted hosts: {error}"));
    drop(host);
    stop_notifier(&mut notifier);
}

#[test]
fn untouched_recovery_is_definitely_unsent() {
    let (mut host, mut notifier) = host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(20), plan())
        .unwrap_or_else(|error| panic!("admit API 91: {error:?}"));
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover host: {error}"));
    let AlterShareGroupOffsetsOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovery: {error}"))
    else {
        panic!("failure expected");
    };
    assert_eq!(
        (failure.kind(), failure.delivery()),
        (
            AlterShareGroupOffsetsFailureKind::DriverRejected,
            AlterShareGroupOffsetsDeliveryStatus::NotSent,
        )
    );
    let _progress = host
        .turn(Moment::from_tick(3))
        .unwrap_or_else(|error| panic!("reclaim turn: {error}"));
    assert_eq!(host.retained_bytes_for_test(), 0);

    drop(host);
    stop_notifier(&mut notifier);
}

#[test]
fn handed_off_without_a_returned_call_cannot_forge_recovery_evidence() {
    let (mut host, mut notifier) = host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(20), plan())
        .unwrap_or_else(|error| panic!("admit API 91: {error:?}"));
    let AlterShareGroupOffsetsTurn::Submit(_submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("handoff turn: {error}"))
    else {
        panic!("submission expected");
    };

    assert!(matches!(
        host.recover_after_driver_shutdown(),
        Err(AlterShareGroupOffsetsHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(&mut notifier);
}

#[test]
fn expired_public_deadline_never_reaches_driver_handoff() {
    let (mut host, mut notifier) = host();
    let admission = host
        .try_admit(Moment::from_tick(10), deadline(10), plan())
        .unwrap_or_else(|error| panic!("admit elapsed API 91: {error:?}"));
    let AlterShareGroupOffsetsOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe elapsed terminal: {error}"))
    else {
        panic!("deadline failure expected");
    };

    assert_eq!(
        (failure.kind(), failure.delivery()),
        (
            AlterShareGroupOffsetsFailureKind::DeadlineElapsed,
            AlterShareGroupOffsetsDeliveryStatus::NotSent,
        )
    );
    assert!(matches!(
        host.turn(Moment::from_tick(11)),
        Ok(AlterShareGroupOffsetsTurn::Progress)
    ));
    assert_eq!(host.retained_bytes_for_test(), 0);

    drop(host);
    stop_notifier(&mut notifier);
}

#[test]
fn recovered_call_remains_retained_when_core_rejects_terminal_fact() {
    let (mut host, mut notifier) = host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(20), plan())
        .unwrap_or_else(|error| panic!("admit API 91: {error:?}"));
    host.retain_recovered_call_for_test();

    assert!(matches!(
        host.settle_recovered_transport_for_test(),
        Err(AlterShareGroupOffsetsHostError::Machine(
            AlterShareGroupOffsetsMachineError::InvalidState
        ))
    ));
    assert!(host.recovered_call_is_retained_for_test());

    drop((admission, host));
    stop_notifier(&mut notifier);
}

#[test]
fn completion_fault_retains_call_until_post_driver_recovery() {
    let (mut host, mut notifier) = host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(20), plan())
        .unwrap_or_else(|error| panic!("admit API 91: {error:?}"));
    let AlterShareGroupOffsetsTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("submission turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, submitted_deadline, submitted_plan, _result_limit) = submission.into_parts();
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call = AlterShareGroupOffsetsCall::submit(
        &driver,
        &submitted_plan,
        submitted_deadline.transport(),
    )
    .unwrap_or_else(|error| panic!("accepted call: {error}"));
    host.accept_call(operation_id, call)
        .unwrap_or_else(|error| panic!("host acceptance: {error}"));
    drop(driver);

    assert!(matches!(
        host.turn(Moment::from_tick(3)),
        Err(AlterShareGroupOffsetsHostError::CallCompletion)
    ));
    assert_eq!(
        host.unsettled(),
        1,
        "completion failure must retain accepted call evidence"
    );
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("post-driver recovery: {error}"));
    let AlterShareGroupOffsetsOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovery: {error}"))
    else {
        panic!("recovery failure expected");
    };
    assert_eq!(
        (failure.kind(), failure.delivery()),
        (
            AlterShareGroupOffsetsFailureKind::Transport,
            AlterShareGroupOffsetsDeliveryStatus::PossiblySent,
        )
    );

    drop(host);
    stop_notifier(&mut notifier);
}

fn host() -> (AlterShareGroupOffsetsHost, AdminCompletionNotifier) {
    let (notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    (
        AlterShareGroupOffsetsHost::new(ports.alter_share_group_offsets),
        notifier,
    )
}

fn plan() -> AlterShareGroupOffsetsPlan {
    AlterShareGroupOffsetsPlan::new(
        "payments-share".to_owned(),
        vec![
            AlterShareGroupOffset::new("orders".to_owned(), 1, 42),
            AlterShareGroupOffset::new("audit".to_owned(), 0, 7),
        ],
    )
    .unwrap_or_else(|error| panic!("valid API-91 plan: {error}"))
}

fn deadline(tick: u64) -> OperationDeadline {
    OperationDeadline::from_parts_for_test(
        kafka_client_core::Deadline::from_tick(tick),
        Instant::now() + Duration::from_secs(1),
    )
}

fn stop_notifier(notifier: &mut AdminCompletionNotifier) {
    notifier
        .stop()
        .unwrap_or_else(|error| panic!("stop notifier: {error}"))
        .join_off_notifier()
        .unwrap_or_else(|_| panic!("join notifier"));
}
