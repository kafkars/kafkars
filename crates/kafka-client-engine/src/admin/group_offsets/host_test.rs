//! Retained-byte, deadline, batch re-arming, recovery, and reclamation scenarios.

use std::time::Instant;

use kafka_client_core::{
    ListConsumerGroupOffsetsBatch as CoreOffsetsBatch, ListConsumerGroupOffsetsInput,
    ListConsumerGroupOffsetsPlan, Moment,
};

use crate::clock::OperationDeadline;

use super::ListConsumerGroupBatchOutcome;
use super::{
    ListConsumerGroupOffsetsAdmissionErrorKind, ListConsumerGroupOffsetsDeliveryStatus,
    ListConsumerGroupOffsetsFailureKind, ListConsumerGroupOffsetsOutcome,
    ListConsumerGroupOffsetsTurn, host::LIST_CONSUMER_GROUP_OFFSETS_RETAINED_BYTES,
};

#[test]
fn one_query_atomically_reserves_the_complete_four_mib_envelope() {
    let (mut host, notifier) = crate::admin::test_support::list_consumer_group_offsets_host();
    let deadline = deadline(10);
    let admission = host
        .try_admit(Moment::from_tick(1), deadline, plan())
        .unwrap_or_else(|error| panic!("admit group offsets: {error:?}"));

    assert!(admission.fault.is_none());
    assert_eq!(host.unsettled(), 1);
    assert_eq!(
        host.retained_bytes_for_test(),
        LIST_CONSUMER_GROUP_OFFSETS_RETAINED_BYTES
    );
    assert_eq!(host.next_deadline(), Some(deadline.core()));
    assert!(matches!(
        host.try_admit(Moment::from_tick(1), deadline, plan()),
        Err(ListConsumerGroupOffsetsAdmissionErrorKind::RetainedBytes)
    ));

    let ListConsumerGroupOffsetsTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take group-offset submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (_operation_id, submitted_deadline, submitted_plan, result_limit) = submission.into_parts();
    assert_eq!(submitted_deadline, deadline);
    assert_eq!(submitted_plan, plan());
    assert!(result_limit < LIST_CONSUMER_GROUP_OFFSETS_RETAINED_BYTES);
    assert!(result_limit > LIST_CONSUMER_GROUP_OFFSETS_RETAINED_BYTES / 2);
    assert_eq!(host.next_deadline(), None);

    drop((admission, host));
    crate::admin::test_support::stop_notifier(notifier);
}

#[test]
fn handed_off_operation_leaves_deadline_parking_to_the_driver() {
    let (mut host, notifier) = crate::admin::test_support::list_consumer_group_offsets_host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(10), plan())
        .unwrap_or_else(|error| panic!("admit group offsets: {error:?}"));
    assert_eq!(host.next_deadline(), Some(deadline(10).core()));
    let ListConsumerGroupOffsetsTurn::Submit(_submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take group-offset submission: {error}"))
    else {
        panic!("submission expected");
    };
    assert_eq!(host.next_deadline(), None);

    drop((admission, host));
    crate::admin::test_support::stop_notifier(notifier);
}

#[test]
fn abandoned_observer_retains_bytes_until_terminal_publication_is_reclaimed() {
    let (mut host, notifier) = crate::admin::test_support::list_consumer_group_offsets_host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(10), plan())
        .unwrap_or_else(|error| panic!("admit group offsets: {error:?}"));
    let ListConsumerGroupOffsetsTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take group-offset submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, _deadline, plan, result_limit) = submission.into_parts();
    drop(admission.observer);
    host.reject_handoff(operation_id, plan, result_limit)
        .unwrap_or_else(|error| panic!("publish rejected handoff: {error}"));
    assert_eq!(
        host.retained_bytes_for_test(),
        LIST_CONSUMER_GROUP_OFFSETS_RETAINED_BYTES
    );

    crate::admin::test_support::stop_notifier(notifier);
    assert!(matches!(
        host.turn(Moment::from_tick(3)),
        Ok(ListConsumerGroupOffsetsTurn::Progress)
    ));
    assert_eq!(host.retained_bytes_for_test(), 0);
    assert!(matches!(
        host.turn(Moment::from_tick(4)),
        Ok(ListConsumerGroupOffsetsTurn::Idle)
    ));
}

#[test]
fn untouched_recovery_is_definitely_unsent() {
    let (mut host, notifier) = crate::admin::test_support::list_consumer_group_offsets_host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(10), plan())
        .unwrap_or_else(|error| panic!("admit group offsets: {error:?}"));

    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover group-offset host: {error}"));
    let ListConsumerGroupOffsetsOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovery: {error}"))
    else {
        panic!("recovery failure expected");
    };
    assert_eq!(
        (failure.kind(), failure.delivery()),
        (
            ListConsumerGroupOffsetsFailureKind::DriverRejected,
            ListConsumerGroupOffsetsDeliveryStatus::NotSent,
        )
    );
    let _progress = host
        .turn(Moment::from_tick(3))
        .unwrap_or_else(|error| panic!("reclaim group-offset terminal: {error}"));
    assert_eq!(host.retained_bytes_for_test(), 0);

    drop(host);
    crate::admin::test_support::stop_notifier(notifier);
}

#[test]
fn expired_call_boundary_deadline_never_reaches_driver_handoff() {
    let (mut host, notifier) = crate::admin::test_support::list_consumer_group_offsets_host();
    let admission = host
        .try_admit(Moment::from_tick(10), deadline(10), plan())
        .unwrap_or_else(|error| panic!("admit elapsed group offsets: {error:?}"));
    let ListConsumerGroupOffsetsOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe elapsed terminal: {error}"))
    else {
        panic!("deadline failure expected");
    };
    assert_eq!(
        (failure.kind(), failure.delivery()),
        (
            ListConsumerGroupOffsetsFailureKind::DeadlineElapsed,
            ListConsumerGroupOffsetsDeliveryStatus::NotSent,
        )
    );
    assert!(matches!(
        host.turn(Moment::from_tick(11)),
        Ok(ListConsumerGroupOffsetsTurn::Progress)
    ));
    assert_eq!(host.retained_bytes_for_test(), 0);

    drop(host);
    crate::admin::test_support::stop_notifier(notifier);
}

#[test]
fn one_batch_rearms_singleton_coordinator_submissions_and_debits_cumulative_results() {
    let (mut host, notifier) = crate::admin::test_support::list_consumer_group_offsets_host();
    let operation_deadline = deadline(50);
    let admission = host
        .try_admit(Moment::from_tick(1), operation_deadline, batch_plan())
        .unwrap_or_else(|error| panic!("admit offset batch: {error:?}"));

    let ListConsumerGroupOffsetsTurn::Submit(first) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take first submission: {error}"))
    else {
        panic!("first singleton submission expected");
    };
    let (operation_id, first_deadline, first_plan, first_limit) = first.into_parts();
    assert_eq!(first_deadline, operation_deadline);
    assert_eq!(first_plan.group_ids(), ["z-readers"]);
    host.apply_for_test(
        operation_id,
        ListConsumerGroupOffsetsInput::DriverAccepted,
        0,
    )
    .unwrap_or_else(|error| panic!("accept first call: {error}"));
    host.apply_for_test(
        operation_id,
        ListConsumerGroupOffsetsInput::BrokerRejected {
            code: core::num::NonZeroI16::new(-719).unwrap_or_else(|| panic!("nonzero")),
            throttle_time_ms: 41,
        },
        128,
    )
    .unwrap_or_else(|error| panic!("settle first call: {error}"));

    let ListConsumerGroupOffsetsTurn::Submit(second) = host
        .turn(Moment::from_tick(3))
        .unwrap_or_else(|error| panic!("take second submission: {error}"))
    else {
        panic!("second singleton submission expected");
    };
    let (second_id, second_deadline, second_plan, second_limit) = second.into_parts();
    assert_eq!(second_id, operation_id);
    assert_eq!(second_deadline, first_deadline);
    assert_eq!(second_plan.group_ids(), ["a-readers"]);
    assert_eq!(second_limit, first_limit - 128);
    host.apply_for_test(
        operation_id,
        ListConsumerGroupOffsetsInput::DriverAccepted,
        0,
    )
    .and_then(|_| {
        host.apply_for_test(
            operation_id,
            ListConsumerGroupOffsetsInput::BrokerResponded {
                batch: CoreOffsetsBatch::new(17, Vec::new()),
            },
            96,
        )
    })
    .unwrap_or_else(|error| panic!("settle second call: {error}"));

    let ListConsumerGroupOffsetsOutcome::Batch(batch) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe offset batch: {error}"))
    else {
        panic!("batch terminal expected");
    };
    assert_eq!(batch.throttle_time_ms(), 41);
    assert_eq!(batch.outcomes().len(), 2);
    assert!(matches!(
        &batch.outcomes()[0],
        ListConsumerGroupBatchOutcome::BrokerRejected {
            group_id,
            code: -719,
        } if group_id == "z-readers"
    ));
    assert!(matches!(
        &batch.outcomes()[1],
        ListConsumerGroupBatchOutcome::Offsets { group_id, .. }
            if group_id == "a-readers"
    ));

    let _progress = host
        .turn(Moment::from_tick(4))
        .unwrap_or_else(|error| panic!("reclaim batch terminal: {error}"));
    assert_eq!(host.retained_bytes_for_test(), 0);
    drop(host);
    crate::admin::test_support::stop_notifier(notifier);
}

fn plan() -> ListConsumerGroupOffsetsPlan {
    ListConsumerGroupOffsetsPlan::new("payments".to_owned(), true)
        .unwrap_or_else(|error| panic!("valid group-offset plan: {error}"))
}

fn batch_plan() -> ListConsumerGroupOffsetsPlan {
    ListConsumerGroupOffsetsPlan::new_batch(
        vec!["z-readers".to_owned(), "a-readers".to_owned()],
        true,
    )
    .unwrap_or_else(|error| panic!("valid batch group-offset plan: {error}"))
}

pub(super) fn deadline(tick: u64) -> OperationDeadline {
    OperationDeadline::from_parts_for_test(
        kafka_client_core::Deadline::from_tick(tick),
        Instant::now() + std::time::Duration::from_secs(1),
    )
}
