//! Admission, shutdown recovery, and retained-envelope scenarios.

use std::sync::Arc;

use kafka_client_core::UnregisterBrokerMachineError;

use crate::{
    EngineConfig,
    admin::AdminCompletionNotifier,
    clock::MonotonicClock,
    driver::{DriverOwner, UnregisterBrokerCall},
};

use super::{
    UnregisterBrokerAdmissionErrorKind, UnregisterBrokerDeliveryStatus,
    UnregisterBrokerFailureKind, UnregisterBrokerHost, UnregisterBrokerHostError,
    UnregisterBrokerOutcome, UnregisterBrokerTurn,
    host::{UNREGISTER_BROKER_RESULT_BYTES, UNREGISTER_BROKER_RETAINED_BYTES},
};

#[test]
fn admission_reserves_terminal_and_four_kib_result_before_submission() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = UnregisterBrokerHost::new(ports.unregister_broker);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan(7))
        .unwrap_or_else(|error| panic!("admit unregistration: {error:?}"));
    assert!(admission.fault.is_none());
    let operation_bytes = host.retained_bytes_for_test();
    assert!(operation_bytes > UNREGISTER_BROKER_RESULT_BYTES);
    assert!(operation_bytes < UNREGISTER_BROKER_RETAINED_BYTES);

    let UnregisterBrokerTurn::Submit(submission) = host
        .turn(capture.now(), None)
        .unwrap_or_else(|error| panic!("submission turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, submitted_deadline, submitted_plan, result_limit) = submission.into_parts();
    assert_eq!(submitted_deadline, capture.operation_deadline());
    assert_eq!(submitted_plan.broker_id(), 7);
    assert_eq!(result_limit, UNREGISTER_BROKER_RESULT_BYTES);

    drop(admission.observer);
    host.reject_handoff(operation_id)
        .unwrap_or_else(|error| panic!("reject inspected handoff: {error}"));
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover host: {error}"));
    drop(host);
    stop_notifier(&mut notifier);
}

#[test]
fn capacity_is_bounded_independently_of_the_aggregate_envelope() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = UnregisterBrokerHost::new(ports.unregister_broker);
    let capture = deadline();
    let mut observers = Vec::new();
    for broker_id in 0..super::UNREGISTER_BROKER_CAPACITY {
        let broker_id = i32::try_from(broker_id).unwrap_or_else(|_| panic!("bounded broker id"));
        observers.push(
            host.try_admit(capture.now(), capture.operation_deadline(), plan(broker_id))
                .unwrap_or_else(|error| panic!("admit {broker_id}: {error:?}"))
                .observer,
        );
    }
    assert!(matches!(
        host.try_admit(capture.now(), capture.operation_deadline(), plan(99)),
        Err(UnregisterBrokerAdmissionErrorKind::Capacity)
    ));
    drop(observers);
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover host: {error}"));
    drop(host);
    stop_notifier(&mut notifier);
}

#[test]
fn untouched_shutdown_is_definitely_unsent_and_reclaimable() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = UnregisterBrokerHost::new(ports.unregister_broker);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan(7))
        .unwrap_or_else(|error| panic!("admit unregistration: {error:?}"));

    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover untouched request: {error}"));
    let UnregisterBrokerOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovery: {error}"))
    else {
        panic!("failure expected");
    };
    assert_eq!(failure.kind(), UnregisterBrokerFailureKind::DriverRejected);
    assert_eq!(failure.delivery(), UnregisterBrokerDeliveryStatus::NotSent);

    let _progress = host
        .turn(capture.now(), None)
        .unwrap_or_else(|error| panic!("reclaim turn: {error}"));
    assert_eq!(host.retained_bytes_for_test(), 0);
    drop(host);
    stop_notifier(&mut notifier);
}

#[test]
fn handed_off_without_a_returned_call_cannot_forge_recovery_evidence() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = UnregisterBrokerHost::new(ports.unregister_broker);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan(7))
        .unwrap_or_else(|error| panic!("admit unregistration: {error:?}"));
    let UnregisterBrokerTurn::Submit(_submission) = host
        .turn(capture.now(), None)
        .unwrap_or_else(|error| panic!("handoff turn: {error}"))
    else {
        panic!("submission expected");
    };

    assert!(matches!(
        host.recover_after_driver_shutdown(),
        Err(UnregisterBrokerHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(&mut notifier);
}

#[test]
fn recovered_call_and_broker_correlation_survive_core_rejection() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = UnregisterBrokerHost::new(ports.unregister_broker);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan(7))
        .unwrap_or_else(|error| panic!("admit unregistration: {error:?}"));
    host.retain_recovered_call_for_test(plan(7));

    assert!(matches!(
        host.settle_recovered_transport_for_test(),
        Err(UnregisterBrokerHostError::Machine(
            UnregisterBrokerMachineError::InvalidState
        ))
    ));
    assert_eq!(host.recovered_broker_id_for_test(), Some(7));
    assert!(matches!(
        host.publish_terminal_for_test(),
        Err(UnregisterBrokerHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(&mut notifier);
}

#[test]
fn completion_fault_retains_call_and_broker_correlation_until_recovery() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = UnregisterBrokerHost::new(ports.unregister_broker);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan(7))
        .unwrap_or_else(|error| panic!("admit unregistration: {error:?}"));
    let UnregisterBrokerTurn::Submit(submission) = host
        .turn(capture.now(), None)
        .unwrap_or_else(|error| panic!("handoff turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, submitted_deadline, submitted_plan, _result_limit) = submission.into_parts();
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call =
        UnregisterBrokerCall::submit(&driver, submitted_plan, submitted_deadline.transport())
            .unwrap_or_else(|_error| panic!("accepted call"));
    host.accept_call(operation_id, call)
        .unwrap_or_else(|error| panic!("host acceptance: {error}"));
    drop(driver);

    assert!(matches!(
        host.turn(capture.now(), None),
        Err(UnregisterBrokerHostError::CallCompletion)
    ));
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("post-driver recovery: {error}"));
    let UnregisterBrokerOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovery: {error}"))
    else {
        panic!("failure expected");
    };
    assert_eq!(failure.kind(), UnregisterBrokerFailureKind::Transport);
    assert_eq!(
        failure.delivery(),
        UnregisterBrokerDeliveryStatus::PossiblySent
    );

    drop(host);
    stop_notifier(&mut notifier);
}

fn plan(broker_id: i32) -> kafka_client_core::UnregisterBrokerPlan {
    kafka_client_core::UnregisterBrokerPlan::new(broker_id)
        .unwrap_or_else(|error| panic!("plan: {error}"))
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
