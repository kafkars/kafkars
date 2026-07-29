//! Admission, deadline, recovery, and retained-envelope scenarios for API 90.

use std::time::{Duration, Instant};

use core::num::NonZeroI16;

use kafka_client_core::{
    ListShareGroupOffsetTarget, ListShareGroupOffsetsBatch as CoreBatch,
    ListShareGroupOffsetsBrokerError as CoreBrokerError, ListShareGroupOffsetsInput,
    ListShareGroupOffsetsPlan, ListShareGroupOffsetsQuery, Moment,
};

use crate::{admin::AdminCompletionNotifier, clock::OperationDeadline};

use super::{
    ListShareGroupOffsetsAdmissionErrorKind, ListShareGroupOffsetsBatchOutcome,
    ListShareGroupOffsetsDeliveryStatus, ListShareGroupOffsetsFailureKind,
    ListShareGroupOffsetsHost, ListShareGroupOffsetsOutcome, ListShareGroupOffsetsTurn,
    host::{LIST_SHARE_GROUP_OFFSETS_RESULT_BYTES, LIST_SHARE_GROUP_OFFSETS_RETAINED_BYTES},
};

#[test]
fn admission_reserves_request_owner_and_complete_result_before_submission() {
    let (mut host, mut notifier) = host();
    let deadline = deadline(20);
    let admission = host
        .try_admit(Moment::from_tick(1), deadline, plan())
        .unwrap_or_else(|error| panic!("admit API 90: {error:?}"));

    assert!(admission.fault.is_none());
    assert_eq!(host.unsettled(), 1);
    assert!(host.retained_bytes_for_test() > LIST_SHARE_GROUP_OFFSETS_RESULT_BYTES);
    assert!(host.retained_bytes_for_test() < LIST_SHARE_GROUP_OFFSETS_RETAINED_BYTES);
    let ListShareGroupOffsetsTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("submission turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (_operation_id, submitted_deadline, submitted_plan, result_limit) = submission.into_parts();
    assert_eq!(submitted_deadline, deadline);
    assert_eq!(submitted_plan, plan());
    assert_eq!(result_limit, LIST_SHARE_GROUP_OFFSETS_RESULT_BYTES);
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
            Err(ListShareGroupOffsetsAdmissionErrorKind::RetainedBytes) => break,
            Err(error) => panic!("unexpected admission failure: {error:?}"),
        }
    }

    assert!(!accepted.is_empty());
    assert!(host.retained_bytes_for_test() <= LIST_SHARE_GROUP_OFFSETS_RETAINED_BYTES);
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
            .unwrap_or_else(|error| panic!("admit API 90: {error:?}"));
        if handed_off {
            let ListShareGroupOffsetsTurn::Submit(_submission) = host
                .turn(Moment::from_tick(2))
                .unwrap_or_else(|error| panic!("handoff turn: {error}"))
            else {
                panic!("submission expected");
            };
        }
        host.recover_after_driver_shutdown()
            .unwrap_or_else(|error| panic!("recover host: {error}"));
        let ListShareGroupOffsetsOutcome::Failed(failure) = admission
            .observer
            .wait()
            .unwrap_or_else(|error| panic!("observe recovery: {error}"))
        else {
            panic!("failure expected");
        };
        let expected = if handed_off {
            (
                ListShareGroupOffsetsFailureKind::Transport,
                ListShareGroupOffsetsDeliveryStatus::PossiblySent,
            )
        } else {
            (
                ListShareGroupOffsetsFailureKind::DriverRejected,
                ListShareGroupOffsetsDeliveryStatus::NotSent,
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
        .unwrap_or_else(|error| panic!("admit elapsed API 90: {error:?}"));
    let ListShareGroupOffsetsOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe elapsed terminal: {error}"))
    else {
        panic!("deadline failure expected");
    };

    assert_eq!(
        (failure.kind(), failure.delivery()),
        (
            ListShareGroupOffsetsFailureKind::DeadlineElapsed,
            ListShareGroupOffsetsDeliveryStatus::NotSent,
        )
    );
    assert!(matches!(
        host.turn(Moment::from_tick(11)),
        Ok(ListShareGroupOffsetsTurn::Progress)
    ));
    assert_eq!(host.retained_bytes_for_test(), 0);

    drop(host);
    stop_notifier(&mut notifier);
}

#[test]
fn one_batch_reservation_rearms_calls_and_publishes_one_ordered_terminal() {
    let (mut host, mut notifier) = host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(20), batch_plan())
        .unwrap_or_else(|error| panic!("admit batch: {error:?}"));
    let ListShareGroupOffsetsTurn::Submit(first) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("first turn: {error}"))
    else {
        panic!("first submission expected");
    };
    let (operation_id, original_deadline, first_plan, first_limit) = first.into_parts();
    assert_eq!(first_plan.group_id(), "share-a");
    host.apply_for_test(operation_id, ListShareGroupOffsetsInput::DriverAccepted, 0)
        .unwrap_or_else(|error| panic!("accept first: {error}"));
    host.apply_for_test(
        operation_id,
        ListShareGroupOffsetsInput::BrokerRejected {
            error: CoreBrokerError::new(
                41,
                NonZeroI16::new(15).unwrap_or_else(|| panic!("nonzero")),
                Some("not coordinator".to_owned()),
                false,
            ),
        },
        32,
    )
    .unwrap_or_else(|error| panic!("settle first: {error}"));

    let ListShareGroupOffsetsTurn::Submit(second) = host
        .turn(Moment::from_tick(3))
        .unwrap_or_else(|error| panic!("second turn: {error}"))
    else {
        panic!("second submission expected");
    };
    let (second_id, second_deadline, second_plan, second_limit) = second.into_parts();
    assert_eq!(second_id, operation_id);
    assert_eq!(second_deadline, original_deadline);
    assert_eq!(second_plan.group_id(), "share-b");
    assert_eq!(second_limit, first_limit - 32);
    host.apply_for_test(operation_id, ListShareGroupOffsetsInput::DriverAccepted, 0)
        .unwrap_or_else(|error| panic!("accept second: {error}"));
    host.apply_for_test(
        operation_id,
        ListShareGroupOffsetsInput::BrokerResponded {
            batch: CoreBatch::new(17, Vec::new()),
        },
        64,
    )
    .unwrap_or_else(|error| panic!("settle second: {error}"));

    let ListShareGroupOffsetsOutcome::Batch(batch) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe batch: {error}"))
    else {
        panic!("batch outcome expected");
    };
    assert_eq!(batch.throttle_time_ms(), 41);
    assert!(matches!(
        &batch.outcomes()[0],
        ListShareGroupOffsetsBatchOutcome::BrokerRejected { group_id, error }
            if group_id == "share-a" && error.code() == 15
    ));
    assert!(matches!(
        &batch.outcomes()[1],
        ListShareGroupOffsetsBatchOutcome::Offsets { group_id, offsets }
            if group_id == "share-b" && offsets.throttle_time_ms() == 17
    ));

    let _reclaim = host
        .turn(Moment::from_tick(4))
        .unwrap_or_else(|error| panic!("reclaim: {error}"));
    assert_eq!(host.retained_bytes_for_test(), 0);
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

fn batch_plan() -> ListShareGroupOffsetsPlan {
    ListShareGroupOffsetsPlan::batch(vec![
        ListShareGroupOffsetsQuery::selected(
            "share-a".to_owned(),
            vec![ListShareGroupOffsetTarget::new("orders".to_owned(), 2)],
        )
        .unwrap_or_else(|error| panic!("first query: {error}")),
        ListShareGroupOffsetsQuery::all("share-b".to_owned())
            .unwrap_or_else(|error| panic!("second query: {error}")),
    ])
    .unwrap_or_else(|error| panic!("batch plan: {error}"))
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
