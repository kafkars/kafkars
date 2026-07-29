//! Accepted-call completion and post-driver recovery ownership scenarios.

use std::time::{Duration, Instant};

use kafka_client_core::{
    ListShareGroupOffsetTarget, ListShareGroupOffsetsMachineError, ListShareGroupOffsetsPlan,
    Moment,
};

use crate::{admin::AdminCompletionNotifier, clock::OperationDeadline};

use super::{
    ListShareGroupOffsetsDeliveryStatus, ListShareGroupOffsetsFailureKind,
    ListShareGroupOffsetsHost, ListShareGroupOffsetsHostError, ListShareGroupOffsetsOutcome,
    ListShareGroupOffsetsTurn,
};

#[test]
fn recovered_call_remains_retained_when_core_rejects_terminal_fact() {
    let (mut host, mut notifier) = host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(), plan())
        .unwrap_or_else(|error| panic!("admit API 90: {error:?}"));
    host.retain_recovered_call_for_test();

    assert!(matches!(
        host.settle_recovered_transport_for_test(),
        Err(ListShareGroupOffsetsHostError::Machine(
            ListShareGroupOffsetsMachineError::InvalidState
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
        .try_admit(Moment::from_tick(1), deadline(), plan())
        .unwrap_or_else(|error| panic!("admit API 90: {error:?}"));
    let ListShareGroupOffsetsTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("handoff turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, _submitted_deadline, _submitted_plan, _result_limit) =
        submission.into_parts();
    let driver = host.install_live_call_for_test(operation_id);
    drop(driver);

    assert!(matches!(
        host.turn(Moment::from_tick(3)),
        Err(ListShareGroupOffsetsHostError::CallCompletion)
    ));
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("post-driver recovery: {error}"));
    let ListShareGroupOffsetsOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("recovery observation: {error}"))
    else {
        panic!("recovery failure expected");
    };
    assert_eq!(
        (failure.kind(), failure.delivery()),
        (
            ListShareGroupOffsetsFailureKind::Transport,
            ListShareGroupOffsetsDeliveryStatus::PossiblySent,
        )
    );

    drop(host);
    stop_notifier(&mut notifier);
}

fn host() -> (ListShareGroupOffsetsHost, AdminCompletionNotifier) {
    let (notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    (
        ListShareGroupOffsetsHost::new(ports.list_share_group_offsets),
        notifier,
    )
}

fn plan() -> ListShareGroupOffsetsPlan {
    ListShareGroupOffsetsPlan::selected(
        "payments-share".to_owned(),
        vec![
            ListShareGroupOffsetTarget::new("orders".to_owned(), 2),
            ListShareGroupOffsetTarget::new("audit".to_owned(), 1),
        ],
    )
    .unwrap_or_else(|error| panic!("valid API-90 plan: {error}"))
}

fn deadline() -> OperationDeadline {
    OperationDeadline::from_parts_for_test(
        kafka_client_core::Deadline::from_tick(20),
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
