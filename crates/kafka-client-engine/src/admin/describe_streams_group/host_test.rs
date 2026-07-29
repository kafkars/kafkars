//! Admission, deadline, recovery, and retained-envelope scenarios for API 89.

use std::time::{Duration, Instant};

use core::num::NonZeroI16;

use kafka_client_core::{
    DescribeStreamsGroupBrokerError, DescribeStreamsGroupDescription, DescribeStreamsGroupInput,
    DescribeStreamsGroupPlan, DescribeStreamsGroupResult, Moment,
};

use crate::{admin::AdminCompletionNotifier, clock::OperationDeadline};

use super::{
    DescribeStreamsGroupAdmissionErrorKind, DescribeStreamsGroupBatchOutcome,
    DescribeStreamsGroupDeliveryStatus, DescribeStreamsGroupFailureKind, DescribeStreamsGroupHost,
    DescribeStreamsGroupOutcome, DescribeStreamsGroupTurn,
    host::{DESCRIBE_STREAMS_GROUP_RESULT_BYTES, DESCRIBE_STREAMS_GROUP_RETAINED_BYTES},
};

#[test]
fn admission_reserves_request_owner_and_complete_result_before_submission() {
    let (mut host, mut notifier) = host();
    let deadline = deadline(20);
    let admission = host
        .try_admit(Moment::from_tick(1), deadline, plan())
        .unwrap_or_else(|error| panic!("admit API 89: {error:?}"));

    assert!(admission.fault.is_none());
    assert_eq!(host.unsettled(), 1);
    assert!(host.retained_bytes_for_test() > DESCRIBE_STREAMS_GROUP_RESULT_BYTES);
    assert!(host.retained_bytes_for_test() < DESCRIBE_STREAMS_GROUP_RETAINED_BYTES);
    let DescribeStreamsGroupTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("submission turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, submitted_deadline, submitted_plan, result_limit) = submission.into_parts();
    assert_eq!(submitted_deadline, deadline);
    assert_eq!(submitted_plan, plan());
    assert_eq!(result_limit, DESCRIBE_STREAMS_GROUP_RESULT_BYTES);
    assert_eq!(host.next_deadline(), None);

    drop(admission.observer);
    host.reject_handoff(operation_id)
        .unwrap_or_else(|error| panic!("reject handoff: {error}"));
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
            Err(DescribeStreamsGroupAdmissionErrorKind::RetainedBytes) => break,
            Err(error) => panic!("unexpected admission failure: {error:?}"),
        }
    }

    assert!(!accepted.is_empty());
    assert!(host.retained_bytes_for_test() <= DESCRIBE_STREAMS_GROUP_RETAINED_BYTES);
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
        .unwrap_or_else(|error| panic!("admit API 89: {error:?}"));
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover host: {error}"));
    let DescribeStreamsGroupOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovery: {error}"))
    else {
        panic!("failure expected");
    };
    assert_eq!(
        (failure.kind(), failure.delivery()),
        (
            DescribeStreamsGroupFailureKind::DriverRejected,
            DescribeStreamsGroupDeliveryStatus::NotSent,
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
fn expired_public_deadline_never_reaches_driver_handoff() {
    let (mut host, mut notifier) = host();
    let admission = host
        .try_admit(Moment::from_tick(10), deadline(10), plan())
        .unwrap_or_else(|error| panic!("admit elapsed API 89: {error:?}"));
    let DescribeStreamsGroupOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe elapsed terminal: {error}"))
    else {
        panic!("deadline failure expected");
    };

    assert_eq!(
        (failure.kind(), failure.delivery()),
        (
            DescribeStreamsGroupFailureKind::DeadlineElapsed,
            DescribeStreamsGroupDeliveryStatus::NotSent,
        )
    );
    assert!(matches!(
        host.turn(Moment::from_tick(11)),
        Ok(DescribeStreamsGroupTurn::Progress)
    ));
    assert_eq!(host.retained_bytes_for_test(), 0);

    drop(host);
    stop_notifier(&mut notifier);
}

#[test]
fn batch_rearms_two_calls_without_early_publication_and_keeps_max_throttle() {
    let (mut host, mut notifier) = host();
    let deadline = deadline(40);
    let admission = host
        .try_admit(Moment::from_tick(1), deadline, batch_plan())
        .unwrap_or_else(|error| panic!("admit API-89 batch: {error:?}"));

    let DescribeStreamsGroupTurn::Submit(first) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("first submission turn: {error}"))
    else {
        panic!("first submission expected");
    };
    let (operation_id, first_deadline, first_plan, first_limit) = first.into_parts();
    assert_eq!(first_deadline, deadline);
    assert_eq!(first_plan.group_id(), "orders");
    assert_eq!(first_plan.group_ids().len(), 1);

    let diagnostic = "coordinator rejected".to_owned();
    let first_charge = "orders".len() + diagnostic.len();
    host.settle_current_for_test(
        operation_id,
        DescribeStreamsGroupInput::BrokerRejected {
            error: DescribeStreamsGroupBrokerError::new(
                29,
                NonZeroI16::new(15).unwrap_or_else(|| panic!("nonzero")),
                Some(diagnostic),
                false,
            ),
        },
        first_charge,
    )
    .unwrap_or_else(|error| panic!("settle first group: {error}"));

    assert_eq!(host.unsettled(), 1, "batch published before all groups");
    let DescribeStreamsGroupTurn::Submit(second) = host
        .turn(Moment::from_tick(3))
        .unwrap_or_else(|error| panic!("second submission turn: {error}"))
    else {
        panic!("second submission expected");
    };
    let (second_operation_id, second_deadline, second_plan, second_limit) = second.into_parts();
    assert_eq!(second_operation_id, operation_id);
    assert_eq!(second_deadline, deadline);
    assert_eq!(second_plan.group_id(), "audit");
    assert_eq!(second_plan.group_ids().len(), 1);
    assert_eq!(second_limit, first_limit - first_charge);

    host.settle_current_for_test(
        operation_id,
        DescribeStreamsGroupInput::BrokerResponded {
            result: described("audit", 7),
        },
        128,
    )
    .unwrap_or_else(|error| panic!("settle second group: {error}"));

    let DescribeStreamsGroupOutcome::Batch(batch) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe batch: {error}"))
    else {
        panic!("batch expected");
    };
    assert_eq!(batch.throttle_time_ms(), 29);
    assert_eq!(
        batch
            .outcomes()
            .iter()
            .map(DescribeStreamsGroupBatchOutcome::group_id)
            .collect::<Vec<_>>(),
        vec!["orders", "audit"]
    );
    let DescribeStreamsGroupBatchOutcome::BrokerRejected { error, .. } = &batch.outcomes()[0]
    else {
        panic!("first group rejection expected");
    };
    assert_eq!((error.throttle_time_ms(), error.code()), (29, 15));
    assert!(matches!(
        &batch.outcomes()[1],
        DescribeStreamsGroupBatchOutcome::Described(_)
    ));

    drop(host);
    stop_notifier(&mut notifier);
}

fn host() -> (DescribeStreamsGroupHost, AdminCompletionNotifier) {
    let (notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    (
        DescribeStreamsGroupHost::new(ports.describe_streams_group),
        notifier,
    )
}

fn plan() -> DescribeStreamsGroupPlan {
    DescribeStreamsGroupPlan::new("payments-streams".to_owned(), false, false)
        .unwrap_or_else(|error| panic!("valid API-89 plan: {error}"))
}

fn batch_plan() -> DescribeStreamsGroupPlan {
    DescribeStreamsGroupPlan::new_batch(vec!["orders".to_owned(), "audit".to_owned()], false, false)
        .unwrap_or_else(|error| panic!("valid API-89 batch plan: {error}"))
}

fn described(group_id: &str, throttle_time_ms: u32) -> DescribeStreamsGroupResult {
    DescribeStreamsGroupResult::new(
        throttle_time_ms,
        DescribeStreamsGroupDescription::new(
            group_id.to_owned(),
            "Stable".to_owned(),
            1,
            1,
            None,
            Vec::new(),
            None,
            None,
            None,
        ),
    )
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
