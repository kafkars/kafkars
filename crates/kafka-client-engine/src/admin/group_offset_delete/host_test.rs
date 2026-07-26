//! Combined-envelope, deadline, recovery, and reclamation scenarios.

use std::time::Instant;

use kafka_client_core::{DeleteConsumerGroupOffsetTarget, DeleteConsumerGroupOffsetsPlan, Moment};

use crate::clock::OperationDeadline;

use super::{
    DeleteConsumerGroupOffsetsAdmissionErrorKind, DeleteConsumerGroupOffsetsDeliveryStatus,
    DeleteConsumerGroupOffsetsFailureKind, DeleteConsumerGroupOffsetsOutcome,
    DeleteConsumerGroupOffsetsTurn, host::DELETE_CONSUMER_GROUP_OFFSETS_RETAINED_BYTES,
};

#[test]
fn admission_atomically_reserves_the_complete_four_mib_envelope() {
    let (mut host, notifier) = crate::admin::test_support::delete_consumer_group_offsets_host();
    let deadline = deadline(10);
    let admission = host
        .try_admit(Moment::from_tick(1), deadline, plan())
        .unwrap_or_else(|error| panic!("admit offset deletion: {error:?}"));

    assert!(admission.fault.is_none());
    assert_eq!(host.unsettled(), 1);
    assert_eq!(
        host.retained_bytes_for_test(),
        DELETE_CONSUMER_GROUP_OFFSETS_RETAINED_BYTES
    );
    assert!(matches!(
        host.try_admit(Moment::from_tick(1), deadline, plan()),
        Err(DeleteConsumerGroupOffsetsAdmissionErrorKind::RetainedBytes)
    ));
    let DeleteConsumerGroupOffsetsTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take deletion submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (_id, submitted_deadline, submitted_plan, scratch_limit) = submission.into_parts();
    assert_eq!(submitted_deadline, deadline);
    assert_eq!(submitted_plan, plan());
    assert!(scratch_limit > DELETE_CONSUMER_GROUP_OFFSETS_RETAINED_BYTES / 2);
    assert_eq!(host.next_deadline(), None);

    drop((admission, host));
    crate::admin::test_support::stop_notifier(notifier);
}

#[test]
fn untouched_and_handed_off_recovery_preserve_delivery_boundary() {
    for handed_off in [false, true] {
        let (mut host, notifier) = crate::admin::test_support::delete_consumer_group_offsets_host();
        let admission = host
            .try_admit(Moment::from_tick(1), deadline(10), plan())
            .unwrap_or_else(|error| panic!("admit offset deletion: {error:?}"));
        if handed_off {
            let DeleteConsumerGroupOffsetsTurn::Submit(_submission) = host
                .turn(Moment::from_tick(2))
                .unwrap_or_else(|error| panic!("take deletion submission: {error}"))
            else {
                panic!("submission expected");
            };
        }
        host.recover_after_driver_shutdown()
            .unwrap_or_else(|error| panic!("recover deletion host: {error}"));
        let DeleteConsumerGroupOffsetsOutcome::Failed(failure) = admission
            .observer
            .wait()
            .unwrap_or_else(|error| panic!("observe recovery: {error}"))
        else {
            panic!("recovery failure expected");
        };
        let expected = if handed_off {
            (
                DeleteConsumerGroupOffsetsFailureKind::Transport,
                DeleteConsumerGroupOffsetsDeliveryStatus::PossiblySent,
            )
        } else {
            (
                DeleteConsumerGroupOffsetsFailureKind::DriverRejected,
                DeleteConsumerGroupOffsetsDeliveryStatus::NotSent,
            )
        };
        assert_eq!((failure.kind(), failure.delivery()), expected);
        let _progress = host
            .turn(Moment::from_tick(3))
            .unwrap_or_else(|error| panic!("reclaim deletion terminal: {error}"));
        assert_eq!(host.retained_bytes_for_test(), 0);

        drop(host);
        crate::admin::test_support::stop_notifier(notifier);
    }
}

#[test]
fn expired_public_deadline_never_reaches_driver_handoff() {
    let (mut host, notifier) = crate::admin::test_support::delete_consumer_group_offsets_host();
    let admission = host
        .try_admit(Moment::from_tick(10), deadline(10), plan())
        .unwrap_or_else(|error| panic!("admit elapsed deletion: {error:?}"));
    let DeleteConsumerGroupOffsetsOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe elapsed terminal: {error}"))
    else {
        panic!("deadline failure expected");
    };
    assert_eq!(
        (failure.kind(), failure.delivery()),
        (
            DeleteConsumerGroupOffsetsFailureKind::DeadlineElapsed,
            DeleteConsumerGroupOffsetsDeliveryStatus::NotSent,
        )
    );
    assert!(matches!(
        host.turn(Moment::from_tick(11)),
        Ok(DeleteConsumerGroupOffsetsTurn::Progress)
    ));
    assert_eq!(host.retained_bytes_for_test(), 0);

    drop(host);
    crate::admin::test_support::stop_notifier(notifier);
}

fn plan() -> DeleteConsumerGroupOffsetsPlan {
    DeleteConsumerGroupOffsetsPlan::new(
        "payments".to_owned(),
        vec![
            DeleteConsumerGroupOffsetTarget::new("orders".to_owned(), 2),
            DeleteConsumerGroupOffsetTarget::new("audit".to_owned(), 0),
        ],
    )
    .unwrap_or_else(|error| panic!("valid offset-deletion plan: {error}"))
}

fn deadline(tick: u64) -> OperationDeadline {
    OperationDeadline::from_parts_for_test(
        kafka_client_core::Deadline::from_tick(tick),
        Instant::now() + std::time::Duration::from_secs(1),
    )
}
