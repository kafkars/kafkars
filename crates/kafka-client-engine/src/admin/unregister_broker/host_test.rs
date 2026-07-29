//! Admission, shutdown recovery, and retained-envelope scenarios.

use std::sync::Arc;

use crate::{admin::AdminCompletionNotifier, clock::MonotonicClock};

use super::{
    UnregisterBrokerAdmissionErrorKind, UnregisterBrokerDeliveryStatus,
    UnregisterBrokerFailureKind, UnregisterBrokerHost, UnregisterBrokerOutcome,
    UnregisterBrokerTurn,
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
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("submission turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (_id, submitted_deadline, submitted_plan, result_limit) = submission.into_parts();
    assert_eq!(submitted_deadline, capture.operation_deadline());
    assert_eq!(submitted_plan.broker_id(), 7);
    assert_eq!(result_limit, UNREGISTER_BROKER_RESULT_BYTES);

    drop(admission.observer);
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
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("reclaim turn: {error}"));
    assert_eq!(host.retained_bytes_for_test(), 0);
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
