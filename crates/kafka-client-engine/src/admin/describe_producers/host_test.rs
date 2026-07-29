//! Retained-envelope, deadline, recovery, and reclamation scenarios.

use std::sync::Arc;

use kafka_client_core::{
    AdminDescribeProducerTarget, AdminDescribeProducersMachineError, AdminDescribeProducersPlan,
    Moment,
};

use crate::{
    EngineConfig,
    admin::{AdminCompletionNotifier, AdminDescribeProducersHost},
    clock::MonotonicClock,
    driver::{DescribeProducersCall, DriverOwner},
};

use super::{
    AdminDescribeProducersAdmissionErrorKind, AdminDescribeProducersDeliveryStatus,
    AdminDescribeProducersFailureKind, AdminDescribeProducersHostError,
    AdminDescribeProducersOutcome, AdminDescribeProducersTurn,
    host::ADMIN_DESCRIBE_PRODUCERS_RETAINED_BYTES,
};

#[test]
fn admission_reserves_terminal_and_full_envelope_before_first_target() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = AdminDescribeProducersHost::new(ports.describe_producers);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan())
        .unwrap_or_else(|error| panic!("admit DescribeProducers: {error:?}"));
    assert!(admission.fault.is_none());
    assert_eq!(
        host.retained_bytes_for_test(),
        ADMIN_DESCRIBE_PRODUCERS_RETAINED_BYTES
    );
    assert_eq!(
        host.next_deadline(),
        Some(capture.operation_deadline().core())
    );
    assert!(matches!(
        host.try_admit(capture.now(), capture.operation_deadline(), plan()),
        Err(AdminDescribeProducersAdmissionErrorKind::RetainedBytes)
    ));

    let AdminDescribeProducersTurn::Submit(submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("submission turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, submitted_deadline, target, broker_id) = submission.into_parts();
    assert_eq!(submitted_deadline, capture.operation_deadline());
    assert_eq!((target.topic(), target.partition()), ("orders", 2));
    assert_eq!(broker_id, Some(7));
    assert_eq!(host.next_deadline(), None);

    drop(admission.observer);
    host.reject_handoff(operation_id)
        .unwrap_or_else(|error| panic!("reject handoff: {error}"));
    drop(host);
    stop_notifier(&mut notifier);
}

#[test]
fn untouched_recovery_is_definitely_unsent() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = AdminDescribeProducersHost::new(ports.describe_producers);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan())
        .unwrap_or_else(|error| panic!("admit DescribeProducers: {error:?}"));

    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover host: {error}"));
    let AdminDescribeProducersOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovery: {error}"))
    else {
        panic!("failure expected");
    };
    assert_eq!(
        (failure.kind(), failure.delivery()),
        (
            AdminDescribeProducersFailureKind::DriverRejected,
            AdminDescribeProducersDeliveryStatus::NotSent,
        )
    );
    let _progress = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("reclaim terminal: {error}"));
    assert_eq!(host.retained_bytes_for_test(), 0);

    drop(host);
    stop_notifier(&mut notifier);
}

#[test]
fn handed_off_without_a_returned_call_cannot_forge_recovery_evidence() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = AdminDescribeProducersHost::new(ports.describe_producers);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan())
        .unwrap_or_else(|error| panic!("admit DescribeProducers: {error:?}"));
    let AdminDescribeProducersTurn::Submit(_submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("take submission: {error}"))
    else {
        panic!("submission expected");
    };

    assert!(matches!(
        host.recover_after_driver_shutdown(),
        Err(AdminDescribeProducersHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(&mut notifier);
}

#[test]
fn recovered_call_remains_retained_when_core_rejects_terminal_fact() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = AdminDescribeProducersHost::new(ports.describe_producers);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan())
        .unwrap_or_else(|error| panic!("admit DescribeProducers: {error:?}"));
    host.retain_recovered_call_for_test();

    assert!(matches!(
        host.settle_recovered_transport_for_test(),
        Err(AdminDescribeProducersHostError::Machine(
            AdminDescribeProducersMachineError::InvalidState
        ))
    ));
    assert!(host.recovered_call_is_retained_for_test());

    drop((admission, host));
    stop_notifier(&mut notifier);
}

#[test]
fn completion_fault_retains_call_until_post_driver_recovery() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = AdminDescribeProducersHost::new(ports.describe_producers);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan())
        .unwrap_or_else(|error| panic!("admit DescribeProducers: {error:?}"));
    let AdminDescribeProducersTurn::Submit(submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("take submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, submitted_deadline, target, broker_id) = submission.into_parts();
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call =
        DescribeProducersCall::submit(&driver, &target, broker_id, submitted_deadline.transport())
            .unwrap_or_else(|_error| panic!("accepted call"));
    host.accept_call(operation_id, call)
        .unwrap_or_else(|error| panic!("host acceptance: {error}"));
    drop(driver);

    assert!(matches!(
        host.turn(Moment::from_tick(3)),
        Err(AdminDescribeProducersHostError::CallCompletion)
    ));
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("post-driver recovery: {error}"));
    let AdminDescribeProducersOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovery: {error}"))
    else {
        panic!("recovery failure expected");
    };
    assert_eq!(
        (failure.kind(), failure.delivery()),
        (
            AdminDescribeProducersFailureKind::Transport,
            AdminDescribeProducersDeliveryStatus::PossiblySent,
        )
    );

    drop(host);
    stop_notifier(&mut notifier);
}

fn plan() -> AdminDescribeProducersPlan {
    AdminDescribeProducersPlan::new(
        vec![
            AdminDescribeProducerTarget::new("orders".to_owned(), 2),
            AdminDescribeProducerTarget::new("audit".to_owned(), 0),
        ],
        Some(7),
    )
    .unwrap_or_else(|error| panic!("valid DescribeProducers plan: {error}"))
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
