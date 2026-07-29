//! Admission, shutdown recovery, and retained-envelope scenarios.

use std::sync::Arc;

use kafka_client_core::{
    ClientQuotaMatch, DescribeClientQuotaFilterComponent, DescribeClientQuotasMachineError,
    DescribeClientQuotasPlan,
};

use crate::{
    EngineConfig,
    admin::{AdminCompletionNotifier, DescribeClientQuotasHost},
    clock::MonotonicClock,
    driver::{DescribeClientQuotasCall, DriverOwner},
};

use super::{
    DescribeClientQuotasAdmissionErrorKind, DescribeClientQuotasDeliveryStatus,
    DescribeClientQuotasFailureKind, DescribeClientQuotasHostError, DescribeClientQuotasOutcome,
    DescribeClientQuotasTurn, host::DESCRIBE_CLIENT_QUOTAS_RETAINED_BYTES,
};

#[test]
fn admission_reserves_terminal_and_full_envelope_before_submission() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = DescribeClientQuotasHost::new(ports.describe_client_quotas);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan("alice"))
        .unwrap_or_else(|error| panic!("admit quota query: {error:?}"));
    assert!(admission.fault.is_none());
    assert_eq!(
        host.retained_bytes_for_test(),
        DESCRIBE_CLIENT_QUOTAS_RETAINED_BYTES
    );
    assert_eq!(
        host.next_deadline(),
        Some(capture.operation_deadline().core())
    );
    assert!(matches!(
        host.try_admit(capture.now(), capture.operation_deadline(), plan("bob"),),
        Err(DescribeClientQuotasAdmissionErrorKind::RetainedBytes)
    ));

    let DescribeClientQuotasTurn::Submit(submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("submission turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, submitted_deadline, submitted_plan, request_scratch_limit, result_limit) =
        submission.into_parts();
    assert_eq!(submitted_deadline, capture.operation_deadline());
    assert_eq!(submitted_plan.components()[0].entity_type(), "user");
    assert_eq!(
        submitted_plan.components()[0].match_kind(),
        &ClientQuotaMatch::Exact("alice".to_owned())
    );
    assert!(result_limit > DESCRIBE_CLIENT_QUOTAS_RETAINED_BYTES / 2);
    assert!(result_limit < DESCRIBE_CLIENT_QUOTAS_RETAINED_BYTES);
    assert!(request_scratch_limit > 0);

    host.reject_handoff(
        operation_id,
        submitted_plan,
        request_scratch_limit,
        result_limit,
    )
    .unwrap_or_else(|error| panic!("reject inspected submission: {error}"));
    drop(admission.observer);
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover host: {error}"));
    drop(host);
    stop_notifier(&mut notifier);
}

#[test]
fn untouched_shutdown_is_definitely_unsent_and_reclaimable() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = DescribeClientQuotasHost::new(ports.describe_client_quotas);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan("alice"))
        .unwrap_or_else(|error| panic!("admit quota query: {error:?}"));

    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover untouched query: {error}"));
    let DescribeClientQuotasOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovery: {error}"))
    else {
        panic!("failure expected");
    };
    assert_eq!(
        failure.kind(),
        &DescribeClientQuotasFailureKind::DriverRejected
    );
    assert_eq!(
        failure.delivery(),
        DescribeClientQuotasDeliveryStatus::NotSent
    );

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
    let mut host = DescribeClientQuotasHost::new(ports.describe_client_quotas);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan("alice"))
        .unwrap_or_else(|error| panic!("admit quota query: {error:?}"));
    let DescribeClientQuotasTurn::Submit(_submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("hand off submission: {error}"))
    else {
        panic!("submission expected");
    };

    assert!(matches!(
        host.recover_after_driver_shutdown(),
        Err(DescribeClientQuotasHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(&mut notifier);
}

#[test]
fn recovered_call_remains_retained_when_core_rejects_terminal_fact() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = DescribeClientQuotasHost::new(ports.describe_client_quotas);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan("alice"))
        .unwrap_or_else(|error| panic!("admit quota query: {error:?}"));
    host.retain_recovered_call_for_test();

    assert!(matches!(
        host.settle_recovered_transport_for_test(),
        Err(DescribeClientQuotasHostError::Machine(
            DescribeClientQuotasMachineError::InvalidState
        ))
    ));
    assert!(host.recovered_call_is_retained_for_test());
    assert!(matches!(
        host.publish_terminal_for_test(),
        Err(DescribeClientQuotasHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(&mut notifier);
}

#[test]
fn completion_fault_retains_call_until_post_driver_recovery() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = DescribeClientQuotasHost::new(ports.describe_client_quotas);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan("alice"))
        .unwrap_or_else(|error| panic!("admit quota query: {error:?}"));
    let DescribeClientQuotasTurn::Submit(submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("handoff turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, submitted_deadline, submitted_plan, request_scratch_limit, result_limit) =
        submission.into_parts();
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call = DescribeClientQuotasCall::submit(
        &driver,
        submitted_plan,
        request_scratch_limit,
        result_limit,
        submitted_deadline.transport(),
    )
    .unwrap_or_else(|_error| panic!("accepted call"));
    host.accept_call(operation_id, call)
        .unwrap_or_else(|error| panic!("host acceptance: {error}"));
    drop(driver);

    assert!(matches!(
        host.turn(capture.now()),
        Err(DescribeClientQuotasHostError::CallCompletion)
    ));
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("post-driver recovery: {error}"));
    let DescribeClientQuotasOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("recovery observation: {error}"))
    else {
        panic!("recovery failure expected");
    };
    assert_eq!(failure.kind(), &DescribeClientQuotasFailureKind::Transport);
    assert_eq!(
        failure.delivery(),
        DescribeClientQuotasDeliveryStatus::PossiblySent
    );

    drop(host);
    stop_notifier(&mut notifier);
}

#[test]
fn mismatched_accepted_call_remains_owned_and_blocks_recovery() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = DescribeClientQuotasHost::new(ports.describe_client_quotas);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan("alice"))
        .unwrap_or_else(|error| panic!("admit quota query: {error:?}"));
    let DescribeClientQuotasTurn::Submit(submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("handoff turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, submitted_deadline, _plan, request_limit, result_limit) =
        submission.into_parts();
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call = DescribeClientQuotasCall::submit(
        &driver,
        plan("bob"),
        request_limit,
        result_limit,
        submitted_deadline.transport(),
    )
    .unwrap_or_else(|_error| panic!("accepted mismatched call"));

    assert_eq!(
        host.accept_call(operation_id, call),
        Err(DescribeClientQuotasHostError::SubmissionMismatch)
    );
    drop(driver);
    assert_eq!(
        host.recover_after_driver_shutdown(),
        Err(DescribeClientQuotasHostError::SubmissionMismatch)
    );
    assert_eq!(host.unsettled(), 1);

    drop((admission, host));
    stop_notifier(&mut notifier);
}

fn plan(name: &str) -> DescribeClientQuotasPlan {
    DescribeClientQuotasPlan::new(
        vec![DescribeClientQuotaFilterComponent::new(
            "user".to_owned(),
            ClientQuotaMatch::Exact(name.to_owned()),
        )],
        false,
    )
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
