//! Admission, deadline, shutdown recovery, and retained-envelope scenarios.

use std::sync::Arc;

use kafka_client_core::{DescribeAclsFilter, DescribeAclsMachineError, DescribeAclsPlan, Moment};

use crate::{
    EngineConfig,
    admin::{AdminCompletionNotifier, DescribeAclsHost},
    clock::MonotonicClock,
    driver::{DescribeAclsCall, DriverOwner},
};

use super::{
    DescribeAclsAdmissionErrorKind, DescribeAclsDeliveryStatus, DescribeAclsFailureKind,
    DescribeAclsHostError, DescribeAclsOutcome, DescribeAclsTurn,
    host::DESCRIBE_ACLS_RETAINED_BYTES,
};

#[test]
fn admission_reserves_terminal_and_complete_envelope_before_submission() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = DescribeAclsHost::new(ports.describe_acls);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan("orders"))
        .unwrap_or_else(|error| panic!("admit ACL query: {error:?}"));
    assert!(admission.fault.is_none());
    assert_eq!(host.retained_bytes_for_test(), DESCRIBE_ACLS_RETAINED_BYTES);
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
        Err(DescribeAclsAdmissionErrorKind::RetainedBytes)
    ));

    let DescribeAclsTurn::Submit(submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("submission turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, submitted_deadline, submitted_plan, result_limit) = submission.into_parts();
    assert_eq!(submitted_deadline, capture.operation_deadline());
    assert_eq!(submitted_plan.filter().resource_name(), Some("orders"));
    assert!(result_limit > DESCRIBE_ACLS_RETAINED_BYTES / 2);
    assert!(result_limit < DESCRIBE_ACLS_RETAINED_BYTES);

    drop(admission.observer);
    host.reject_handoff(operation_id, submitted_plan, result_limit)
        .unwrap_or_else(|error| panic!("reject handoff: {error}"));
    drop(host);
    stop_notifier(&mut notifier);
}

#[test]
fn untouched_shutdown_recovery_is_definitely_unsent_and_reclaimable() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = DescribeAclsHost::new(ports.describe_acls);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan("orders"))
        .unwrap_or_else(|error| panic!("admit ACL query: {error:?}"));

    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover untouched query: {error}"));
    let DescribeAclsOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovery: {error}"))
    else {
        panic!("failure expected");
    };
    assert_eq!(failure.kind(), &DescribeAclsFailureKind::DriverRejected);
    assert_eq!(failure.delivery(), DescribeAclsDeliveryStatus::NotSent);

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
    let mut host = DescribeAclsHost::new(ports.describe_acls);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan("orders"))
        .unwrap_or_else(|error| panic!("admit ACL query: {error:?}"));
    let DescribeAclsTurn::Submit(_submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("hand off submission: {error}"))
    else {
        panic!("submission expected");
    };

    assert!(matches!(
        host.recover_after_driver_shutdown(),
        Err(DescribeAclsHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(&mut notifier);
}

#[test]
fn recovered_call_remains_retained_when_core_rejects_terminal_fact() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = DescribeAclsHost::new(ports.describe_acls);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan("orders"))
        .unwrap_or_else(|error| panic!("admit ACL query: {error:?}"));
    host.retain_recovered_call_for_test();

    assert!(matches!(
        host.settle_recovered_transport_for_test(),
        Err(DescribeAclsHostError::Machine(
            DescribeAclsMachineError::InvalidState
        ))
    ));
    assert!(host.recovered_call_is_retained_for_test());
    assert!(matches!(
        host.publish_terminal_for_test(),
        Err(DescribeAclsHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(&mut notifier);
}

#[test]
fn completion_fault_retains_call_until_post_driver_recovery() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = DescribeAclsHost::new(ports.describe_acls);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan("orders"))
        .unwrap_or_else(|error| panic!("admit ACL query: {error:?}"));
    let DescribeAclsTurn::Submit(submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("take submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, submitted_deadline, submitted_plan, result_limit) = submission.into_parts();
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call = DescribeAclsCall::submit(
        &driver,
        submitted_plan,
        result_limit,
        submitted_deadline.transport(),
    )
    .unwrap_or_else(|_error| panic!("accepted call"));
    host.accept_call(operation_id, call)
        .unwrap_or_else(|error| panic!("host acceptance: {error}"));
    drop(driver);

    assert!(matches!(
        host.turn(Moment::from_tick(3)),
        Err(DescribeAclsHostError::CallCompletion)
    ));
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("post-driver recovery: {error}"));
    let DescribeAclsOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovery: {error}"))
    else {
        panic!("recovery failure expected");
    };
    assert_eq!(failure.kind(), &DescribeAclsFailureKind::Transport);
    assert_eq!(failure.delivery(), DescribeAclsDeliveryStatus::PossiblySent);

    drop(host);
    stop_notifier(&mut notifier);
}

fn plan(resource_name: &str) -> DescribeAclsPlan {
    DescribeAclsPlan::new(DescribeAclsFilter::new(
        2,
        Some(resource_name.to_owned()),
        3,
        None,
        None,
        1,
        1,
    ))
    .unwrap_or_else(|error| panic!("valid filter: {error}"))
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
