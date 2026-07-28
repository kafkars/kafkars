//! Admission, deadline, shutdown recovery, and retained-envelope scenarios.

use std::sync::Arc;

use kafka_client_core::{CreateAclBinding, CreateAclsMachineError, CreateAclsPlan};

use crate::{
    EngineConfig,
    admin::{AdminCompletionNotifier, CreateAclsHost},
    clock::MonotonicClock,
    driver::{CreateAclsCall, DriverOwner},
};

use super::{
    CreateAclsAdmissionErrorKind, CreateAclsDeliveryStatus, CreateAclsFailureKind,
    CreateAclsHostError, CreateAclsOutcome, CreateAclsTurn, host::CREATE_ACLS_RETAINED_BYTES,
};

#[test]
fn admission_reserves_terminal_result_slots_and_complete_envelope_before_submission() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = CreateAclsHost::new(ports.create_acls);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan("orders"))
        .unwrap_or_else(|error| panic!("admit ACL creation: {error:?}"));
    assert!(admission.fault.is_none());
    assert_eq!(host.retained_bytes_for_test(), CREATE_ACLS_RETAINED_BYTES);
    assert_eq!(host.prepared_result_capacity_for_test(), Some(1));
    assert_eq!(host.prepared_outcome_capacity_for_test(), Some(1));
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
        Err(CreateAclsAdmissionErrorKind::RetainedBytes)
    ));

    let CreateAclsTurn::Submit(submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("submission turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, submitted_deadline, submitted_plan, request_limit, result_limit) =
        submission.into_parts();
    assert_eq!(submitted_deadline, capture.operation_deadline());
    assert_eq!(submitted_plan.bindings()[0].resource_name(), "orders");
    assert!(request_limit > CREATE_ACLS_RETAINED_BYTES / 2);
    assert!(request_limit < CREATE_ACLS_RETAINED_BYTES);
    assert_eq!(result_limit, request_limit);

    drop(admission.observer);
    assert_eq!(
        host.reject_handoff(operation_id, plan("payments"), request_limit, result_limit),
        Err(CreateAclsHostError::SubmissionMismatch)
    );
    host.reject_handoff(operation_id, submitted_plan, request_limit, result_limit)
        .unwrap_or_else(|error| panic!("reject inspected handoff: {error}"));
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover host: {error}"));
    drop(host);
    stop_notifier(&mut notifier);
}

#[test]
fn untouched_shutdown_recovery_is_definitely_unsent_and_reclaimable() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = CreateAclsHost::new(ports.create_acls);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan("orders"))
        .unwrap_or_else(|error| panic!("admit ACL creation: {error:?}"));

    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover untouched creation: {error}"));
    let CreateAclsOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovery: {error}"))
    else {
        panic!("failure expected");
    };
    assert_eq!(failure.kind(), CreateAclsFailureKind::DriverRejected);
    assert_eq!(failure.delivery(), CreateAclsDeliveryStatus::NotSent);

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
    let mut host = CreateAclsHost::new(ports.create_acls);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan("orders"))
        .unwrap_or_else(|error| panic!("admit ACL creation: {error:?}"));
    let CreateAclsTurn::Submit(_submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("hand off submission: {error}"))
    else {
        panic!("submission expected");
    };

    assert!(matches!(
        host.recover_after_driver_shutdown(),
        Err(CreateAclsHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(&mut notifier);
}

#[test]
fn recovered_call_and_creation_storage_remain_retained_when_core_rejects_terminal_fact() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = CreateAclsHost::new(ports.create_acls);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan("orders"))
        .unwrap_or_else(|error| panic!("admit ACL creation: {error:?}"));
    host.retain_recovered_call_for_test();

    assert!(matches!(
        host.settle_recovered_transport_for_test(),
        Err(CreateAclsHostError::Machine(
            CreateAclsMachineError::InvalidState
        ))
    ));
    assert!(host.recovered_ownership_is_retained_for_test());
    assert!(matches!(
        host.publish_terminal_for_test(),
        Err(CreateAclsHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(&mut notifier);
}

#[test]
fn completion_fault_retains_call_until_post_driver_recovery() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = CreateAclsHost::new(ports.create_acls);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan("orders"))
        .unwrap_or_else(|error| panic!("admit ACL creation: {error:?}"));
    let CreateAclsTurn::Submit(submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("hand off submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, submitted_deadline, submitted_plan, request_limit, result_limit) =
        submission.into_parts();
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call = CreateAclsCall::submit(
        &driver,
        submitted_plan,
        request_limit,
        result_limit,
        submitted_deadline.transport(),
    )
    .unwrap_or_else(|_error| panic!("accepted call"));
    host.accept_call(operation_id, call)
        .unwrap_or_else(|error| panic!("host acceptance: {error}"));
    drop(driver);

    assert!(matches!(
        host.turn(capture.now()),
        Err(CreateAclsHostError::CallCompletion)
    ));
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("post-driver recovery: {error}"));
    let CreateAclsOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("recovery observation: {error}"))
    else {
        panic!("recovery failure expected");
    };
    assert_eq!(failure.kind(), CreateAclsFailureKind::Transport);
    assert_eq!(failure.delivery(), CreateAclsDeliveryStatus::PossiblySent);

    drop(host);
    stop_notifier(&mut notifier);
}

#[test]
fn mismatched_accepted_call_remains_owned_and_blocks_recovery() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = CreateAclsHost::new(ports.create_acls);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan("orders"))
        .unwrap_or_else(|error| panic!("admit ACL creation: {error:?}"));
    let CreateAclsTurn::Submit(submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("hand off submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, submitted_deadline, _plan, request_limit, result_limit) =
        submission.into_parts();
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call = CreateAclsCall::submit(
        &driver,
        plan("payments"),
        request_limit,
        result_limit,
        submitted_deadline.transport(),
    )
    .unwrap_or_else(|_error| panic!("accepted mismatched call"));

    assert_eq!(
        host.accept_call(operation_id, call),
        Err(CreateAclsHostError::SubmissionMismatch)
    );
    drop(driver);
    assert_eq!(
        host.recover_after_driver_shutdown(),
        Err(CreateAclsHostError::SubmissionMismatch)
    );
    assert_eq!(host.unsettled(), 1);

    drop((admission, host));
    stop_notifier(&mut notifier);
}

fn plan(resource_name: &str) -> CreateAclsPlan {
    CreateAclsPlan::new(vec![CreateAclBinding::new(
        2,
        resource_name.to_owned(),
        3,
        "User:alice".to_owned(),
        "*".to_owned(),
        3,
        3,
    )])
    .unwrap_or_else(|error| panic!("valid creation plan: {error}"))
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
