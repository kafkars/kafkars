//! Admission, shutdown recovery, and retained-envelope scenarios.

use std::sync::Arc;

use kafka_client_core::DescribeUserScramCredentialsPlan;

use crate::{
    admin::{AdminCompletionNotifier, DescribeUserScramCredentialsHost},
    clock::MonotonicClock,
};

use super::{
    DescribeUserScramCredentialsAdmissionErrorKind, DescribeUserScramCredentialsDeliveryStatus,
    DescribeUserScramCredentialsFailureKind, DescribeUserScramCredentialsHostError,
    DescribeUserScramCredentialsOutcome, DescribeUserScramCredentialsTurn,
    host::DESCRIBE_USER_SCRAM_CREDENTIALS_RETAINED_BYTES,
};

#[test]
fn admission_reserves_terminal_and_full_envelope_before_submission() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = DescribeUserScramCredentialsHost::new(ports.describe_user_scram_credentials);
    let capture = deadline();
    let admission = host
        .try_admit(
            capture.now(),
            capture.operation_deadline(),
            plan(Some("alice")),
        )
        .unwrap_or_else(|error| panic!("admit SCRAM query: {error:?}"));
    assert!(admission.fault.is_none());
    assert_eq!(
        host.retained_bytes_for_test(),
        DESCRIBE_USER_SCRAM_CREDENTIALS_RETAINED_BYTES
    );
    assert_eq!(
        host.next_deadline(),
        Some(capture.operation_deadline().core())
    );
    assert!(matches!(
        host.try_admit(
            capture.now(),
            capture.operation_deadline(),
            plan(Some("bob")),
        ),
        Err(DescribeUserScramCredentialsAdmissionErrorKind::RetainedBytes)
    ));

    let DescribeUserScramCredentialsTurn::Submit(submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("submission turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, submitted_deadline, submitted_plan, request_limit, result_limit) =
        submission.into_parts();
    assert_eq!(submitted_deadline, capture.operation_deadline());
    assert_eq!(
        submitted_plan.users(),
        Some(["alice".to_owned()].as_slice())
    );
    assert!(result_limit > DESCRIBE_USER_SCRAM_CREDENTIALS_RETAINED_BYTES / 2);
    assert!(result_limit < DESCRIBE_USER_SCRAM_CREDENTIALS_RETAINED_BYTES);
    assert_eq!(request_limit, result_limit);

    host.reject_handoff(operation_id, submitted_plan, request_limit, result_limit)
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
    let mut host = DescribeUserScramCredentialsHost::new(ports.describe_user_scram_credentials);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan(None))
        .unwrap_or_else(|error| panic!("admit SCRAM query: {error:?}"));

    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover untouched query: {error}"));
    let DescribeUserScramCredentialsOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovery: {error}"))
    else {
        panic!("failure expected");
    };
    assert_eq!(
        failure.kind(),
        &DescribeUserScramCredentialsFailureKind::DriverRejected
    );
    assert_eq!(
        failure.delivery(),
        DescribeUserScramCredentialsDeliveryStatus::NotSent
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
    let mut host = DescribeUserScramCredentialsHost::new(ports.describe_user_scram_credentials);
    let capture = deadline();
    let admission = host
        .try_admit(
            capture.now(),
            capture.operation_deadline(),
            plan(Some("alice")),
        )
        .unwrap_or_else(|error| panic!("admit SCRAM query: {error:?}"));
    let DescribeUserScramCredentialsTurn::Submit(_submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("hand off submission: {error}"))
    else {
        panic!("submission expected");
    };

    assert!(matches!(
        host.recover_after_driver_shutdown(),
        Err(DescribeUserScramCredentialsHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(&mut notifier);
}

fn plan(user: Option<&str>) -> DescribeUserScramCredentialsPlan {
    DescribeUserScramCredentialsPlan::new(user.map(|user| vec![user.to_owned()]))
        .unwrap_or_else(|error| panic!("valid user selection: {error}"))
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
