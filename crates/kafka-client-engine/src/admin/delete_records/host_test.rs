//! Retained-envelope, deadline, recovery, and reclamation scenarios.

use std::time::Instant;

use kafka_client_core::{
    DeleteRecordsMachineError, DeleteRecordsPlan, DeleteRecordsTarget, Moment,
};

use crate::{
    EngineConfig,
    clock::OperationDeadline,
    driver::{DeleteRecordsCall, DriverOwner},
};

use super::{
    DeleteRecordsAdmissionErrorKind, DeleteRecordsDeliveryStatus, DeleteRecordsFailureKind,
    DeleteRecordsHostError, DeleteRecordsOutcome, DeleteRecordsTurn,
    host::DELETE_RECORDS_RETAINED_BYTES,
};

#[test]
fn one_query_atomically_reserves_the_complete_envelope_and_first_target() {
    let (mut host, notifier) = crate::admin::test_support::delete_records_host();
    let deadline = deadline(10);
    let admission = host
        .try_admit(Moment::from_tick(1), deadline, plan())
        .unwrap_or_else(|error| panic!("admit DeleteRecords: {error:?}"));

    assert!(admission.fault.is_none());
    assert_eq!(host.unsettled(), 1);
    assert_eq!(
        host.retained_bytes_for_test(),
        DELETE_RECORDS_RETAINED_BYTES
    );
    assert_eq!(host.next_deadline(), Some(deadline.core()));
    assert!(matches!(
        host.try_admit(Moment::from_tick(1), deadline, plan()),
        Err(DeleteRecordsAdmissionErrorKind::RetainedBytes)
    ));

    let DeleteRecordsTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take DeleteRecords submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (_operation_id, submitted_deadline, target) = submission.into_parts();
    assert_eq!(submitted_deadline, deadline);
    assert_eq!(target.topic(), "orders");
    assert_eq!(target.partition(), 2);
    assert_eq!(target.before_offset(), 91);
    assert_eq!(host.next_deadline(), None);

    drop((admission, host));
    crate::admin::test_support::stop_notifier(notifier);
}

#[test]
fn abandoned_observer_retains_bytes_until_terminal_publication_is_reclaimed() {
    let (mut host, notifier) = crate::admin::test_support::delete_records_host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(10), plan())
        .unwrap_or_else(|error| panic!("admit DeleteRecords: {error:?}"));
    let DeleteRecordsTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take DeleteRecords submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, _deadline, _target) = submission.into_parts();
    drop(admission.observer);
    host.reject_handoff(operation_id)
        .unwrap_or_else(|error| panic!("publish rejected handoff: {error}"));
    assert_eq!(
        host.retained_bytes_for_test(),
        DELETE_RECORDS_RETAINED_BYTES
    );

    crate::admin::test_support::stop_notifier(notifier);
    assert!(matches!(
        host.turn(Moment::from_tick(3)),
        Ok(DeleteRecordsTurn::Progress)
    ));
    assert_eq!(host.retained_bytes_for_test(), 0);
}

#[test]
fn untouched_shutdown_recovery_is_definitely_unsent() {
    let (mut host, notifier) = crate::admin::test_support::delete_records_host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(10), plan())
        .unwrap_or_else(|error| panic!("admit DeleteRecords: {error:?}"));

    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover DeleteRecords host: {error}"));
    let DeleteRecordsOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovery: {error}"))
    else {
        panic!("recovery failure expected");
    };
    assert_eq!(
        (failure.kind(), failure.delivery()),
        (
            DeleteRecordsFailureKind::DriverRejected,
            DeleteRecordsDeliveryStatus::NotSent,
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
fn handed_off_without_a_returned_call_cannot_forge_recovery_evidence() {
    let (mut host, notifier) = crate::admin::test_support::delete_records_host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(10), plan())
        .unwrap_or_else(|error| panic!("admit DeleteRecords: {error:?}"));
    let DeleteRecordsTurn::Submit(_submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take DeleteRecords submission: {error}"))
    else {
        panic!("submission expected");
    };

    assert!(matches!(
        host.recover_after_driver_shutdown(),
        Err(DeleteRecordsHostError::InvalidHandoff)
    ));

    drop((admission, host));
    crate::admin::test_support::stop_notifier(notifier);
}

#[test]
fn expired_public_deadline_never_reaches_driver_handoff() {
    let (mut host, notifier) = crate::admin::test_support::delete_records_host();
    let admission = host
        .try_admit(Moment::from_tick(10), deadline(10), plan())
        .unwrap_or_else(|error| panic!("admit elapsed DeleteRecords: {error:?}"));
    let DeleteRecordsOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe elapsed terminal: {error}"))
    else {
        panic!("deadline failure expected");
    };
    assert_eq!(
        (failure.kind(), failure.delivery()),
        (
            DeleteRecordsFailureKind::DeadlineElapsed,
            DeleteRecordsDeliveryStatus::NotSent,
        )
    );
    assert!(matches!(
        host.turn(Moment::from_tick(11)),
        Ok(DeleteRecordsTurn::Progress)
    ));
    assert_eq!(host.retained_bytes_for_test(), 0);

    drop(host);
    crate::admin::test_support::stop_notifier(notifier);
}

#[test]
fn recovered_call_remains_retained_when_core_rejects_terminal_fact() {
    let (mut host, notifier) = crate::admin::test_support::delete_records_host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(10), plan())
        .unwrap_or_else(|error| panic!("admit DeleteRecords: {error:?}"));
    host.retain_recovered_call_for_test();

    assert!(matches!(
        host.settle_recovered_transport_for_test(),
        Err(DeleteRecordsHostError::Machine(
            DeleteRecordsMachineError::InvalidState
        ))
    ));
    assert!(host.recovered_call_is_retained_for_test());

    drop((admission, host));
    crate::admin::test_support::stop_notifier(notifier);
}

#[test]
fn completion_fault_retains_call_until_post_driver_recovery() {
    let (mut host, notifier) = crate::admin::test_support::delete_records_host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(10), one_target_plan())
        .unwrap_or_else(|error| panic!("admit DeleteRecords: {error:?}"));
    let DeleteRecordsTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take DeleteRecords submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, submitted_deadline, target) = submission.into_parts();
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call = DeleteRecordsCall::submit(&driver, &target, 1_000, submitted_deadline.transport())
        .unwrap_or_else(|_error| panic!("accepted call"));
    host.accept_call(operation_id, call)
        .unwrap_or_else(|error| panic!("host acceptance: {error}"));
    drop(driver);

    assert!(matches!(
        host.turn(Moment::from_tick(3)),
        Err(DeleteRecordsHostError::CallCompletion)
    ));
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("post-driver recovery: {error}"));
    let DeleteRecordsOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("recovery observation: {error}"))
    else {
        panic!("recovery failure expected");
    };
    assert_eq!(
        (failure.kind(), failure.delivery()),
        (
            DeleteRecordsFailureKind::Transport,
            DeleteRecordsDeliveryStatus::PossiblySent,
        )
    );

    drop(host);
    crate::admin::test_support::stop_notifier(notifier);
}

fn plan() -> DeleteRecordsPlan {
    DeleteRecordsPlan::new(vec![
        DeleteRecordsTarget::new("orders".to_owned(), 2, 91),
        DeleteRecordsTarget::new("audit".to_owned(), 0, -1),
    ])
    .unwrap_or_else(|error| panic!("valid DeleteRecords plan: {error}"))
}

fn one_target_plan() -> DeleteRecordsPlan {
    DeleteRecordsPlan::new(vec![DeleteRecordsTarget::new("orders".to_owned(), 2, 91)])
        .unwrap_or_else(|error| panic!("valid DeleteRecords plan: {error}"))
}

fn deadline(tick: u64) -> OperationDeadline {
    OperationDeadline::from_parts_for_test(
        kafka_client_core::Deadline::from_tick(tick),
        Instant::now() + std::time::Duration::from_secs(1),
    )
}
