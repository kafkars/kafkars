//! Retained-envelope, deadline, recovery, and reclamation scenarios.

use std::time::{Duration, Instant};

use kafka_client_core::{DeleteConsumerGroupsPlan, DeleteConsumerGroupsTarget, Moment};

use crate::{
    EngineConfig,
    clock::OperationDeadline,
    driver::{DeleteConsumerGroupsCall, DriverOwner},
};

use super::{
    DeleteConsumerGroupsAdmissionErrorKind, DeleteConsumerGroupsDeliveryStatus,
    DeleteConsumerGroupsFailureKind, DeleteConsumerGroupsHostError, DeleteConsumerGroupsOutcome,
    DeleteConsumerGroupsTurn, host::DELETE_CONSUMER_GROUPS_RETAINED_BYTES,
};

#[test]
fn one_query_atomically_reserves_the_complete_envelope_and_first_target() {
    let (mut host, notifier) = crate::admin::test_support::delete_consumer_groups_host();
    let deadline = deadline(10);
    let admission = host
        .try_admit(Moment::from_tick(1), deadline, plan())
        .unwrap_or_else(|error| panic!("admit DeleteConsumerGroups: {error:?}"));

    assert!(admission.fault.is_none());
    assert_eq!(host.unsettled(), 1);
    assert_eq!(
        host.retained_bytes_for_test(),
        DELETE_CONSUMER_GROUPS_RETAINED_BYTES
    );
    assert_eq!(host.next_deadline(), Some(deadline.core()));
    assert!(matches!(
        host.try_admit(Moment::from_tick(1), deadline, plan()),
        Err(DeleteConsumerGroupsAdmissionErrorKind::RetainedBytes)
    ));

    let DeleteConsumerGroupsTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take DeleteConsumerGroups submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (_operation_id, submitted_deadline, submitted_plan, target, request_limit, result_limit) =
        submission.into_parts();
    assert_eq!(submitted_deadline, deadline);
    assert_eq!(submitted_plan, plan());
    assert_eq!(target.group_id(), "orders-workers");
    assert!(request_limit > 0);
    assert!(result_limit > request_limit);
    assert_eq!(host.next_deadline(), None);

    drop((admission, host));
    crate::admin::test_support::stop_notifier(notifier);
}

#[test]
fn abandoned_observer_retains_bytes_until_terminal_publication_is_reclaimed() {
    let (mut host, notifier) = crate::admin::test_support::delete_consumer_groups_host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(10), plan())
        .unwrap_or_else(|error| panic!("admit DeleteConsumerGroups: {error:?}"));
    let DeleteConsumerGroupsTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take DeleteConsumerGroups submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, _deadline, plan, target, request_limit, result_limit) =
        submission.into_parts();
    drop(admission.observer);
    host.reject_handoff(operation_id, plan, target, request_limit, result_limit)
        .unwrap_or_else(|error| panic!("publish rejected handoff: {error}"));
    assert_eq!(
        host.retained_bytes_for_test(),
        DELETE_CONSUMER_GROUPS_RETAINED_BYTES
    );

    crate::admin::test_support::stop_notifier(notifier);
    assert!(matches!(
        host.turn(Moment::from_tick(3)),
        Ok(DeleteConsumerGroupsTurn::Progress)
    ));
    assert_eq!(host.retained_bytes_for_test(), 0);
}

#[test]
fn untouched_shutdown_recovery_is_definitely_unsent() {
    let (mut host, notifier) = crate::admin::test_support::delete_consumer_groups_host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(10), plan())
        .unwrap_or_else(|error| panic!("admit DeleteConsumerGroups: {error:?}"));

    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover DeleteConsumerGroups host: {error}"));
    let DeleteConsumerGroupsOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovery: {error}"))
    else {
        panic!("recovery failure expected");
    };
    assert_eq!(
        (failure.kind(), failure.delivery()),
        (
            DeleteConsumerGroupsFailureKind::DriverRejected,
            DeleteConsumerGroupsDeliveryStatus::NotSent,
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
    let (mut host, notifier) = crate::admin::test_support::delete_consumer_groups_host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(10), plan())
        .unwrap_or_else(|error| panic!("admit DeleteConsumerGroups: {error:?}"));
    let DeleteConsumerGroupsTurn::Submit(_submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take DeleteConsumerGroups submission: {error}"))
    else {
        panic!("submission expected");
    };

    assert!(matches!(
        host.recover_after_driver_shutdown(),
        Err(DeleteConsumerGroupsHostError::InvalidHandoff)
    ));

    drop((admission, host));
    crate::admin::test_support::stop_notifier(notifier);
}

#[test]
fn expired_public_deadline_never_reaches_driver_handoff() {
    let (mut host, notifier) = crate::admin::test_support::delete_consumer_groups_host();
    let admission = host
        .try_admit(Moment::from_tick(10), deadline(10), plan())
        .unwrap_or_else(|error| panic!("admit elapsed DeleteConsumerGroups: {error:?}"));
    let DeleteConsumerGroupsOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe elapsed terminal: {error}"))
    else {
        panic!("deadline failure expected");
    };
    assert_eq!(
        (failure.kind(), failure.delivery()),
        (
            DeleteConsumerGroupsFailureKind::DeadlineElapsed,
            DeleteConsumerGroupsDeliveryStatus::NotSent,
        )
    );
    assert!(matches!(
        host.turn(Moment::from_tick(11)),
        Ok(DeleteConsumerGroupsTurn::Progress)
    ));
    assert_eq!(host.retained_bytes_for_test(), 0);

    drop(host);
    crate::admin::test_support::stop_notifier(notifier);
}

#[test]
fn completion_fault_retains_call_until_post_driver_recovery() {
    let (mut host, notifier) = crate::admin::test_support::delete_consumer_groups_host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(10), one_target_plan())
        .unwrap_or_else(|error| panic!("admit DeleteConsumerGroups: {error:?}"));
    let DeleteConsumerGroupsTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take DeleteConsumerGroups submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, submitted_deadline, plan, target, request_limit, result_limit) =
        submission.into_parts();
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call = DeleteConsumerGroupsCall::submit(
        &driver,
        plan,
        target,
        request_limit,
        result_limit,
        submitted_deadline.transport(),
    )
    .unwrap_or_else(|_error| panic!("accepted call"));
    host.accept_call(operation_id, call)
        .unwrap_or_else(|error| panic!("host acceptance: {error}"));
    drop(driver);

    assert!(matches!(
        host.turn(Moment::from_tick(3)),
        Err(DeleteConsumerGroupsHostError::CallCompletion)
    ));
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("post-driver recovery: {error}"));
    let DeleteConsumerGroupsOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("recovery observation: {error}"))
    else {
        panic!("recovery failure expected");
    };
    assert_eq!(
        (failure.kind(), failure.delivery()),
        (
            DeleteConsumerGroupsFailureKind::Transport,
            DeleteConsumerGroupsDeliveryStatus::PossiblySent,
        )
    );

    drop(host);
    crate::admin::test_support::stop_notifier(notifier);
}

#[test]
fn mismatched_accepted_evidence_survives_into_shutdown_recovery() {
    let (mut host, notifier) = crate::admin::test_support::delete_consumer_groups_host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(10), one_target_plan())
        .unwrap_or_else(|error| panic!("admit DeleteConsumerGroups: {error:?}"));
    let DeleteConsumerGroupsTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, submitted_deadline, _plan, _target, request_limit, result_limit) =
        submission.into_parts();
    let mismatched_target = DeleteConsumerGroupsTarget::new("other-workers".to_owned());
    let mismatched_plan = DeleteConsumerGroupsPlan::new(vec![mismatched_target.clone()])
        .unwrap_or_else(|error| panic!("mismatched plan: {error}"));
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call = DeleteConsumerGroupsCall::submit(
        &driver,
        mismatched_plan,
        mismatched_target,
        request_limit,
        result_limit,
        submitted_deadline.transport(),
    )
    .unwrap_or_else(|_| panic!("accepted mismatched call"));
    assert!(matches!(
        host.accept_call(operation_id, call),
        Err(DeleteConsumerGroupsHostError::SubmissionMismatch)
    ));
    drop(driver);
    assert!(matches!(
        host.recover_after_driver_shutdown(),
        Err(DeleteConsumerGroupsHostError::SubmissionMismatch)
    ));

    drop((admission, host));
    crate::admin::test_support::stop_notifier(notifier);
}

fn plan() -> DeleteConsumerGroupsPlan {
    DeleteConsumerGroupsPlan::new(vec![
        DeleteConsumerGroupsTarget::new("orders-workers".to_owned()),
        DeleteConsumerGroupsTarget::new("audit-workers".to_owned()),
    ])
    .unwrap_or_else(|error| panic!("valid DeleteConsumerGroups plan: {error}"))
}

fn one_target_plan() -> DeleteConsumerGroupsPlan {
    DeleteConsumerGroupsPlan::new(vec![DeleteConsumerGroupsTarget::new(
        "orders-workers".to_owned(),
    )])
    .unwrap_or_else(|error| panic!("valid DeleteConsumerGroups plan: {error}"))
}

fn deadline(tick: u64) -> OperationDeadline {
    OperationDeadline::from_parts_for_test(
        kafka_client_core::Deadline::from_tick(tick),
        Instant::now() + Duration::from_secs(1),
    )
}
