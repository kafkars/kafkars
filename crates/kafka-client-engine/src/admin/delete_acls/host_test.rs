//! Admission, deadline, shutdown recovery, and retained-envelope scenarios.

use std::sync::Arc;

use kafka_client_core::{DeleteAclsFilter, DeleteAclsMachineError, DeleteAclsPlan, Moment};

use crate::{
    EngineConfig,
    admin::{AdminCompletionNotifier, DeleteAclsHost},
    clock::MonotonicClock,
    driver::{DeleteAclsCall, DriverOwner},
};

use super::{
    DeleteAclsAdmissionErrorKind, DeleteAclsDeliveryStatus, DeleteAclsFailureKind,
    DeleteAclsHostError, DeleteAclsOutcome, DeleteAclsTurn, host::DELETE_ACLS_RETAINED_BYTES,
};

#[test]
fn admission_reserves_terminal_known_storage_and_complete_envelope() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = DeleteAclsHost::new(ports.delete_acls);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan("orders"))
        .unwrap_or_else(|error| panic!("admit ACL deletion: {error:?}"));
    assert!(admission.fault.is_none());
    assert_eq!(host.retained_bytes_for_test(), DELETE_ACLS_RETAINED_BYTES);
    assert_eq!(host.prepared_capacities_for_test(), Some((1, 1, 1)));
    assert_eq!(
        host.next_deadline(),
        Some(capture.operation_deadline().core())
    );
    assert!(matches!(
        host.try_admit(
            capture.now(),
            capture.operation_deadline(),
            plan("payments"),
        ),
        Err(DeleteAclsAdmissionErrorKind::RetainedBytes)
    ));

    let DeleteAclsTurn::Submit(submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("submission turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (
        operation_id,
        submitted_deadline,
        submitted_plan,
        request_limit,
        nested_count_capacity,
        result_capacity,
        outcome_capacity,
    ) = submission.into_parts();
    assert_eq!(submitted_deadline, capture.operation_deadline());
    assert_eq!(submitted_plan.filters()[0].resource_name(), Some("orders"));
    assert!(request_limit > DELETE_ACLS_RETAINED_BYTES / 2);
    assert!(request_limit < DELETE_ACLS_RETAINED_BYTES);
    assert_eq!(nested_count_capacity, 1);
    assert_eq!(result_capacity, 1);
    assert_eq!(outcome_capacity, 1);

    drop(admission.observer);
    assert!(matches!(
        host.reject_handoff(
            operation_id,
            submitted_plan.clone(),
            request_limit.saturating_add(1),
            nested_count_capacity,
            result_capacity,
            outcome_capacity,
        ),
        Err(DeleteAclsHostError::SubmissionMismatch)
    ));
    assert_eq!(host.unsettled(), 1);
    host.reject_handoff(
        operation_id,
        submitted_plan,
        request_limit,
        nested_count_capacity,
        result_capacity,
        outcome_capacity,
    )
    .unwrap_or_else(|error| panic!("reject handoff: {error}"));
    drop(host);
    stop_notifier(&mut notifier);
}

#[test]
fn untouched_shutdown_recovery_is_definitely_unsent_and_reclaimable() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = DeleteAclsHost::new(ports.delete_acls);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan("orders"))
        .unwrap_or_else(|error| panic!("admit ACL deletion: {error:?}"));

    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover untouched deletion: {error}"));
    let DeleteAclsOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovery: {error}"))
    else {
        panic!("failure expected");
    };
    assert_eq!(failure.kind(), DeleteAclsFailureKind::DriverRejected);
    assert_eq!(failure.delivery(), DeleteAclsDeliveryStatus::NotSent);

    let _progress = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("reclaim turn: {error}"));
    assert_eq!(host.retained_bytes_for_test(), 0);

    drop(host);
    stop_notifier(&mut notifier);
}

#[test]
fn handed_off_without_a_returned_call_cannot_forge_recovery_evidence() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = DeleteAclsHost::new(ports.delete_acls);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan("orders"))
        .unwrap_or_else(|error| panic!("admit ACL deletion: {error:?}"));
    let DeleteAclsTurn::Submit(_submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("hand off submission: {error}"))
    else {
        panic!("submission expected");
    };

    assert!(matches!(
        host.recover_after_driver_shutdown(),
        Err(DeleteAclsHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(&mut notifier);
}

#[test]
fn recovered_call_and_route_filters_survive_core_rejection() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = DeleteAclsHost::new(ports.delete_acls);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan("orders"))
        .unwrap_or_else(|error| panic!("admit ACL deletion: {error:?}"));
    let DeleteAclsTurn::Submit(submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("hand off submission: {error}"))
    else {
        panic!("submission expected");
    };
    drop(submission);
    host.retain_mismatched_recovered_call_for_test();
    assert!(matches!(
        host.settle_recovered_transport_for_test(),
        Err(DeleteAclsHostError::SubmissionMismatch)
    ));
    assert!(host.has_recovered_ownership_for_test());
    assert!(matches!(
        host.publish_terminal_for_test(),
        Err(DeleteAclsHostError::InvalidHandoff)
    ));
    host.retain_recovered_call_for_test();

    assert!(matches!(
        host.settle_recovered_transport_for_test(),
        Err(DeleteAclsHostError::Machine(
            DeleteAclsMachineError::InvalidState
        ))
    ));
    assert!(host.recovered_ownership_is_retained_for_test());
    assert_eq!(host.prepared_capacities_for_test(), Some((1, 1, 1)));
    assert!(matches!(
        host.publish_terminal_for_test(),
        Err(DeleteAclsHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(&mut notifier);
}

#[test]
fn completion_fault_retains_call_and_filters_until_post_driver_recovery() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = DeleteAclsHost::new(ports.delete_acls);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan("orders"))
        .unwrap_or_else(|error| panic!("admit ACL deletion: {error:?}"));
    let DeleteAclsTurn::Submit(submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("take submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (
        operation_id,
        submitted_deadline,
        route_filters,
        request_limit,
        nested_count_capacity,
        result_capacity,
        outcome_capacity,
    ) = submission.into_parts();
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call = DeleteAclsCall::submit(
        &driver,
        route_filters.clone(),
        request_limit,
        nested_count_capacity,
        result_capacity,
        outcome_capacity,
        submitted_deadline.transport(),
    )
    .unwrap_or_else(|_error| panic!("accepted call"));
    assert!(call.matches(
        &route_filters,
        request_limit,
        nested_count_capacity,
        result_capacity,
        outcome_capacity,
    ));
    host.accept_call(operation_id, call)
        .unwrap_or_else(|error| panic!("host acceptance: {error}"));
    drop(driver);

    assert!(matches!(
        host.turn(Moment::from_tick(capture.now().tick().saturating_add(1))),
        Err(DeleteAclsHostError::CallCompletion)
    ));
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("post-driver recovery: {error}"));
    let DeleteAclsOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovery: {error}"))
    else {
        panic!("recovery failure expected");
    };
    assert_eq!(
        (failure.kind(), failure.delivery()),
        (
            DeleteAclsFailureKind::Transport,
            DeleteAclsDeliveryStatus::PossiblySent,
        )
    );

    drop(host);
    stop_notifier(&mut notifier);
}

fn plan(resource_name: &str) -> DeleteAclsPlan {
    DeleteAclsPlan::new(vec![DeleteAclsFilter::new(
        2,
        Some(resource_name.to_owned()),
        3,
        Some("User:alice".to_owned()),
        Some("*".to_owned()),
        3,
        3,
    )])
    .unwrap_or_else(|error| panic!("valid deletion plan: {error}"))
}

fn deadline() -> crate::clock::DeadlineCapture {
    Arc::new(MonotonicClock::new())
        .capture_deadline_after(std::time::Duration::from_secs(5))
        .unwrap_or_else(|error| panic!("deadline: {error}"))
}

fn stop_notifier(notifier: &mut AdminCompletionNotifier) {
    notifier
        .stop()
        .unwrap_or_else(|error| panic!("stop notifier: {error}"))
        .join_off_notifier()
        .unwrap_or_else(|_| panic!("join notifier"));
}
