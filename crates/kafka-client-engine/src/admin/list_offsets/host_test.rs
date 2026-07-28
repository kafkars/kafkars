//! Retained-envelope, deadline, recovery, and reclamation scenarios.

use std::time::Instant;

use kafka_client_core::{
    AdminListOffsetSpec, AdminListOffsetTarget, AdminListOffsetsPlan, Moment, ReadIsolation,
};

use crate::clock::OperationDeadline;

use super::{
    AdminListOffsetsAdmissionErrorKind, AdminListOffsetsDeliveryStatus,
    AdminListOffsetsFailureKind, AdminListOffsetsHostError, AdminListOffsetsOutcome,
    AdminListOffsetsTurn, host::ADMIN_LIST_OFFSETS_RETAINED_BYTES,
};

#[test]
fn one_query_atomically_reserves_the_complete_envelope_and_first_target() {
    let (mut host, notifier) = crate::admin::test_support::admin_list_offsets_host();
    let deadline = deadline(10);
    let admission = host
        .try_admit(Moment::from_tick(1), deadline, plan())
        .unwrap_or_else(|error| panic!("admit ListOffsets: {error:?}"));

    assert!(admission.fault.is_none());
    assert_eq!(host.unsettled(), 1);
    assert_eq!(
        host.retained_bytes_for_test(),
        ADMIN_LIST_OFFSETS_RETAINED_BYTES
    );
    assert_eq!(host.next_deadline(), Some(deadline.core()));
    assert!(matches!(
        host.try_admit(Moment::from_tick(1), deadline, plan()),
        Err(AdminListOffsetsAdmissionErrorKind::RetainedBytes)
    ));

    let AdminListOffsetsTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take ListOffsets submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (_operation_id, submitted_deadline, target, read_isolation) = submission.into_parts();
    assert_eq!(submitted_deadline, deadline);
    assert_eq!(target.topic(), "orders");
    assert_eq!(target.partition(), 2);
    assert_eq!(target.spec(), AdminListOffsetSpec::Latest);
    assert_eq!(read_isolation, ReadIsolation::ReadUncommitted);
    assert_eq!(host.next_deadline(), None);

    drop((admission, host));
    crate::admin::test_support::stop_notifier(notifier);
}

#[test]
fn abandoned_observer_retains_bytes_until_terminal_publication_is_reclaimed() {
    let (mut host, notifier) = crate::admin::test_support::admin_list_offsets_host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(10), plan())
        .unwrap_or_else(|error| panic!("admit ListOffsets: {error:?}"));
    let AdminListOffsetsTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take ListOffsets submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, _deadline, target, read_isolation) = submission.into_parts();
    drop(admission.observer);
    host.reject_handoff(operation_id, target, read_isolation)
        .unwrap_or_else(|error| panic!("publish rejected handoff: {error}"));
    assert_eq!(
        host.retained_bytes_for_test(),
        ADMIN_LIST_OFFSETS_RETAINED_BYTES
    );

    crate::admin::test_support::stop_notifier(notifier);
    assert!(matches!(
        host.turn(Moment::from_tick(3)),
        Ok(AdminListOffsetsTurn::Progress)
    ));
    assert_eq!(host.retained_bytes_for_test(), 0);
}

#[test]
fn rejected_handoff_requires_exact_target_and_isolation() {
    let (mut host, notifier) = crate::admin::test_support::admin_list_offsets_host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(10), plan())
        .unwrap_or_else(|error| panic!("admit ListOffsets: {error:?}"));
    let AdminListOffsetsTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take ListOffsets submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, _deadline, target, read_isolation) = submission.into_parts();

    assert!(matches!(
        host.reject_handoff(
            operation_id,
            AdminListOffsetTarget::new("other".to_owned(), target.partition(), target.spec(),),
            ReadIsolation::ReadCommitted,
        ),
        Err(AdminListOffsetsHostError::SubmissionMismatch)
    ));
    assert_eq!(host.unsettled(), 1);
    host.reject_handoff(operation_id, target, read_isolation)
        .unwrap_or_else(|error| panic!("reject exact handoff: {error}"));
    let AdminListOffsetsOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe rejected handoff: {error}"))
    else {
        panic!("driver rejection expected");
    };
    assert_eq!(
        (failure.kind(), failure.delivery()),
        (
            AdminListOffsetsFailureKind::DriverRejected,
            AdminListOffsetsDeliveryStatus::NotSent,
        )
    );

    drop(host);
    crate::admin::test_support::stop_notifier(notifier);
}

#[test]
fn untouched_recovery_is_definitely_unsent() {
    let (mut host, notifier) = crate::admin::test_support::admin_list_offsets_host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(10), plan())
        .unwrap_or_else(|error| panic!("admit ListOffsets: {error:?}"));

    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover ListOffsets host: {error}"));
    let AdminListOffsetsOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovery: {error}"))
    else {
        panic!("recovery failure expected");
    };
    assert_eq!(
        (failure.kind(), failure.delivery()),
        (
            AdminListOffsetsFailureKind::DriverRejected,
            AdminListOffsetsDeliveryStatus::NotSent,
        )
    );
    let _progress = host
        .turn(Moment::from_tick(3))
        .unwrap_or_else(|error| panic!("reclaim terminal: {error}"));
    assert_eq!(host.retained_bytes_for_test(), 0);

    drop(host);
    crate::admin::test_support::stop_notifier(notifier);
}

#[test]
fn expired_public_deadline_never_reaches_driver_handoff() {
    let (mut host, notifier) = crate::admin::test_support::admin_list_offsets_host();
    let admission = host
        .try_admit(Moment::from_tick(10), deadline(10), plan())
        .unwrap_or_else(|error| panic!("admit elapsed ListOffsets: {error:?}"));
    let AdminListOffsetsOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe elapsed terminal: {error}"))
    else {
        panic!("deadline failure expected");
    };
    assert_eq!(
        (failure.kind(), failure.delivery()),
        (
            AdminListOffsetsFailureKind::DeadlineElapsed,
            AdminListOffsetsDeliveryStatus::NotSent,
        )
    );
    assert!(matches!(
        host.turn(Moment::from_tick(11)),
        Ok(AdminListOffsetsTurn::Progress)
    ));
    assert_eq!(host.retained_bytes_for_test(), 0);

    drop(host);
    crate::admin::test_support::stop_notifier(notifier);
}

fn plan() -> AdminListOffsetsPlan {
    AdminListOffsetsPlan::new(vec![
        AdminListOffsetTarget::new("orders".to_owned(), 2, AdminListOffsetSpec::Latest),
        AdminListOffsetTarget::new("audit".to_owned(), 0, AdminListOffsetSpec::Earliest),
    ])
    .unwrap_or_else(|error| panic!("valid ListOffsets plan: {error}"))
}

fn deadline(tick: u64) -> OperationDeadline {
    OperationDeadline::from_parts_for_test(
        kafka_client_core::Deadline::from_tick(tick),
        Instant::now() + std::time::Duration::from_secs(1),
    )
}
