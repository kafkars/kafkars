//! Admission, deadline, recovery, and retained-envelope scenarios for API 77.

use core::num::NonZeroI16;
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
    time::{Duration, Instant},
};

use kafka_client_core::{
    DescribeShareGroupBrokerError as CoreBrokerError,
    DescribeShareGroupDescription as CoreDescription, DescribeShareGroupInput,
    DescribeShareGroupPlan, DescribeShareGroupResult as CoreResult, Moment,
};

use crate::{admin::AdminCompletionNotifier, clock::OperationDeadline};

use super::{
    DescribeShareGroupAdmissionErrorKind, DescribeShareGroupBatchOutcome,
    DescribeShareGroupDeliveryStatus, DescribeShareGroupFailureKind, DescribeShareGroupHost,
    DescribeShareGroupObserver, DescribeShareGroupObserverError, DescribeShareGroupOutcome,
    DescribeShareGroupTurn,
    host::{DESCRIBE_SHARE_GROUP_RESULT_BYTES, DESCRIBE_SHARE_GROUP_RETAINED_BYTES},
};

#[test]
fn admission_reserves_request_owner_and_complete_result_before_submission() {
    let (mut host, mut notifier) = host();
    let deadline = deadline(20);
    let admission = host
        .try_admit(Moment::from_tick(1), deadline, plan())
        .unwrap_or_else(|error| panic!("admit API 77: {error:?}"));

    assert!(admission.fault.is_none());
    assert_eq!(host.unsettled(), 1);
    assert!(host.retained_bytes_for_test() > DESCRIBE_SHARE_GROUP_RESULT_BYTES);
    assert!(host.retained_bytes_for_test() < DESCRIBE_SHARE_GROUP_RETAINED_BYTES);
    let DescribeShareGroupTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("submission turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (_operation_id, submitted_deadline, submitted_plan, result_limit) = submission.into_parts();
    assert_eq!(submitted_deadline, deadline);
    assert_eq!(submitted_plan, plan());
    assert_eq!(result_limit, DESCRIBE_SHARE_GROUP_RESULT_BYTES);
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
            Err(DescribeShareGroupAdmissionErrorKind::RetainedBytes) => break,
            Err(error) => panic!("unexpected admission failure: {error:?}"),
        }
    }

    assert!(!accepted.is_empty());
    assert!(host.retained_bytes_for_test() <= DESCRIBE_SHARE_GROUP_RETAINED_BYTES);
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
            .unwrap_or_else(|error| panic!("admit API 77: {error:?}"));
        if handed_off {
            let DescribeShareGroupTurn::Submit(_submission) = host
                .turn(Moment::from_tick(2))
                .unwrap_or_else(|error| panic!("handoff turn: {error}"))
            else {
                panic!("submission expected");
            };
        }
        host.recover_after_driver_shutdown()
            .unwrap_or_else(|error| panic!("recover host: {error}"));
        let DescribeShareGroupOutcome::Failed(failure) = admission
            .observer
            .wait()
            .unwrap_or_else(|error| panic!("observe recovery: {error}"))
        else {
            panic!("failure expected");
        };
        let expected = if handed_off {
            (
                DescribeShareGroupFailureKind::Transport,
                DescribeShareGroupDeliveryStatus::PossiblySent,
            )
        } else {
            (
                DescribeShareGroupFailureKind::DriverRejected,
                DescribeShareGroupDeliveryStatus::NotSent,
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
        .unwrap_or_else(|error| panic!("admit elapsed API 77: {error:?}"));
    let DescribeShareGroupOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe elapsed terminal: {error}"))
    else {
        panic!("deadline failure expected");
    };

    assert_eq!(
        (failure.kind(), failure.delivery()),
        (
            DescribeShareGroupFailureKind::DeadlineElapsed,
            DescribeShareGroupDeliveryStatus::NotSent,
        )
    );
    assert!(matches!(
        host.turn(Moment::from_tick(11)),
        Ok(DescribeShareGroupTurn::Progress)
    ));
    assert_eq!(host.retained_bytes_for_test(), 0);

    drop(host);
    stop_notifier(&mut notifier);
}

#[test]
fn batch_rearms_same_deadline_and_publishes_only_after_every_group() {
    let (mut host, mut notifier) = host();
    let deadline = deadline(20);
    let admission = host
        .try_admit(Moment::from_tick(1), deadline, batch_plan())
        .unwrap_or_else(|error| panic!("admit API-77 batch: {error:?}"));
    let mut observer = admission.observer;

    let DescribeShareGroupTurn::Submit(first) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("first submission turn: {error}"))
    else {
        panic!("first submission expected");
    };
    let (operation_id, first_deadline, first_plan, first_limit) = first.into_parts();
    assert_eq!(first_deadline, deadline);
    assert_eq!(first_plan.group_ids(), &["payments-share".to_owned()]);
    assert_eq!(first_limit, DESCRIBE_SHARE_GROUP_RESULT_BYTES);

    host.apply_for_test(operation_id, DescribeShareGroupInput::DriverAccepted, 0)
        .and_then(|()| {
            host.apply_for_test(
                operation_id,
                DescribeShareGroupInput::BrokerRejected {
                    error: CoreBrokerError::new(
                        23,
                        NonZeroI16::new(15).unwrap_or_else(|| panic!("nonzero")),
                        Some("coordinator moving".to_owned()),
                        false,
                    ),
                },
                64,
            )
        })
        .unwrap_or_else(|error| panic!("settle first group: {error}"));
    assert!(matches!(poll_once(&mut observer), Poll::Pending));

    let DescribeShareGroupTurn::Submit(second) = host
        .turn(Moment::from_tick(3))
        .unwrap_or_else(|error| panic!("second submission turn: {error}"))
    else {
        panic!("second submission expected");
    };
    let (second_id, second_deadline, second_plan, second_limit) = second.into_parts();
    assert_eq!(second_id, operation_id);
    assert_eq!(second_deadline, deadline);
    assert_eq!(second_plan.group_ids(), &["orders-share".to_owned()]);
    assert_eq!(second_limit, first_limit - 64);

    host.apply_for_test(operation_id, DescribeShareGroupInput::DriverAccepted, 0)
        .and_then(|()| {
            host.apply_for_test(
                operation_id,
                DescribeShareGroupInput::BrokerResponded {
                    result: CoreResult::new(
                        3,
                        CoreDescription::new(
                            "orders-share".to_owned(),
                            "Stable".to_owned(),
                            4,
                            5,
                            "uniform".to_owned(),
                            Vec::new(),
                            None,
                        ),
                    ),
                },
                80,
            )
        })
        .unwrap_or_else(|error| panic!("settle second group: {error}"));

    let DescribeShareGroupOutcome::Batch(batch) = observer
        .wait()
        .unwrap_or_else(|error| panic!("observe API-77 batch: {error}"))
    else {
        panic!("batch terminal expected");
    };
    let (maximum_throttle, outcomes) = batch.into_parts();
    assert_eq!(maximum_throttle, 23);
    assert_eq!(outcomes.len(), 2);
    assert!(matches!(
        &outcomes[0],
        DescribeShareGroupBatchOutcome::BrokerRejected { group_id, error }
            if group_id == "payments-share"
                && error.throttle_time_ms() == 23
                && error.code() == 15
    ));
    assert!(matches!(
        &outcomes[1],
        DescribeShareGroupBatchOutcome::Described(result)
            if result.description().group_id == "orders-share"
                && result.throttle_time_ms() == 3
    ));

    assert!(matches!(
        host.turn(Moment::from_tick(4)),
        Ok(DescribeShareGroupTurn::Progress)
    ));
    assert_eq!(host.retained_bytes_for_test(), 0);
    drop(host);
    stop_notifier(&mut notifier);
}

fn host() -> (DescribeShareGroupHost, AdminCompletionNotifier) {
    let (notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    (
        DescribeShareGroupHost::new(ports.describe_share_group),
        notifier,
    )
}

fn plan() -> DescribeShareGroupPlan {
    DescribeShareGroupPlan::new("payments-share".to_owned(), false)
        .unwrap_or_else(|error| panic!("valid API-77 plan: {error}"))
}

fn batch_plan() -> DescribeShareGroupPlan {
    DescribeShareGroupPlan::new_batch(
        vec!["payments-share".to_owned(), "orders-share".to_owned()],
        false,
    )
    .unwrap_or_else(|error| panic!("valid API-77 batch plan: {error}"))
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

fn poll_once(
    observer: &mut DescribeShareGroupObserver,
) -> Poll<Result<DescribeShareGroupOutcome, DescribeShareGroupObserverError>> {
    let waker = Waker::from(Arc::new(NoopWake));
    let mut context = Context::from_waker(&waker);
    Pin::new(observer).poll(&mut context)
}

struct NoopWake;

impl Wake for NoopWake {
    fn wake(self: Arc<Self>) {}
}
