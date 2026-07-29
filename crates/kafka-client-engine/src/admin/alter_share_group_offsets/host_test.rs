//! Admission, deadline, recovery, and retained-envelope scenarios for API 91.

use std::time::{Duration, Instant};

use kafka_client_core::{AlterShareGroupOffset, AlterShareGroupOffsetsPlan, Moment};

use crate::{admin::AdminCompletionNotifier, clock::OperationDeadline};

use super::{
    AlterShareGroupOffsetsAdmissionErrorKind, AlterShareGroupOffsetsDeliveryStatus,
    AlterShareGroupOffsetsFailureKind, AlterShareGroupOffsetsHost, AlterShareGroupOffsetsOutcome,
    AlterShareGroupOffsetsTurn,
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
    let (_operation_id, submitted_deadline, submitted_plan, result_limit) = submission.into_parts();
    assert_eq!(submitted_deadline, deadline);
    assert_eq!(submitted_plan, plan());
    assert_eq!(result_limit, ALTER_SHARE_GROUP_OFFSETS_RESULT_BYTES);
    assert_eq!(host.next_deadline(), None);

    drop(admission.observer);
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover host: {error}"));
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
fn untouched_and_handed_off_recovery_preserve_delivery_boundary() {
    for handed_off in [false, true] {
        let (mut host, mut notifier) = host();
        let admission = host
            .try_admit(Moment::from_tick(1), deadline(20), plan())
            .unwrap_or_else(|error| panic!("admit API 91: {error:?}"));
        if handed_off {
            let AlterShareGroupOffsetsTurn::Submit(_submission) = host
                .turn(Moment::from_tick(2))
                .unwrap_or_else(|error| panic!("handoff turn: {error}"))
            else {
                panic!("submission expected");
            };
        }
        host.recover_after_driver_shutdown()
            .unwrap_or_else(|error| panic!("recover host: {error}"));
        let AlterShareGroupOffsetsOutcome::Failed(failure) = admission
            .observer
            .wait()
            .unwrap_or_else(|error| panic!("observe recovery: {error}"))
        else {
            panic!("failure expected");
        };
        let expected = if handed_off {
            (
                AlterShareGroupOffsetsFailureKind::Transport,
                AlterShareGroupOffsetsDeliveryStatus::PossiblySent,
            )
        } else {
            (
                AlterShareGroupOffsetsFailureKind::DriverRejected,
                AlterShareGroupOffsetsDeliveryStatus::NotSent,
            )
        };
        assert_eq!((failure.kind(), failure.delivery()), expected);
        let _progress = host
            .turn(Moment::from_tick(3))
            .unwrap_or_else(|error| panic!("reclaim turn: {error}"));
        assert_eq!(host.retained_bytes_for_test(), 0);

        drop(host);
        stop_notifier(&mut notifier);
    }
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
