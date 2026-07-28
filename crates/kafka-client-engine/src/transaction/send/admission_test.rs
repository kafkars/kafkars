//! Fixed-slot admission, commit fencing, and local healthy-failure tests.

use std::time::Duration;

use kafka_client_core::{
    CompressionPolicy, DeliveryStatus, Moment, TransactionLifecycleMachineError,
};

use crate::transaction::TransactionLifecycleHostError;

use super::{
    TransactionSendAdmissionFailureKind, TransactionSendFailureKind, TransactionSendTerminal,
    test_support::{
        FakeAggregate, FakeProducePort, deadline, driver, request, request_with_deadline,
    },
};

#[test]
fn accepted_send_blocks_commit_and_busy_restores_the_exact_request() {
    let mut aggregate = FakeAggregate::new();
    let epoch = aggregate.epoch;
    let mut owner = aggregate.send_owner(CompressionPolicy::None);
    let accepted = owner
        .try_send_with(&mut aggregate, request(epoch, "orders", 1_024))
        .unwrap_or_else(|error| panic!("first send is accepted: {error:?}"));

    assert_eq!(accepted.send_id().get(), 1);
    assert!(matches!(
        aggregate.host.commit(epoch, deadline(51)),
        Err(TransactionLifecycleHostError::Core(
            TransactionLifecycleMachineError::OutstandingSends { count: 1 }
        ))
    ));
    let second_deadline = deadline(54);
    let second = request_with_deadline(epoch, "payments", 2_048, second_deadline);
    let Err(failure) = owner.try_send_with(&mut aggregate, second) else {
        panic!("second transactional send unexpectedly acquired the occupied fixed slot");
    };
    assert_eq!(failure.kind(), TransactionSendAdmissionFailureKind::Busy);
    assert_eq!(
        failure.into_input(),
        request_with_deadline(epoch, "payments", 2_048, second_deadline).into_input()
    );
}

#[test]
fn local_enrollment_rejection_settles_failed_healthy_before_returning() {
    let mut aggregate = FakeAggregate::new();
    aggregate.local_enrollment = true;
    let epoch = aggregate.epoch;
    let mut owner = aggregate.send_owner(CompressionPolicy::None);
    let accepted = owner
        .try_send_with(&mut aggregate, request(epoch, "", 1_024))
        .unwrap_or_else(|error| {
            panic!("lifecycle accepted the send before local enrollment rejected it: {error:?}")
        });
    let send_id = accepted.send_id();
    let observer = accepted.into_observer();
    let mut driver = driver();
    let mut port = FakeProducePort::success(&aggregate, send_id);
    owner
        .turn_with(&mut aggregate, Moment::from_tick(1), &driver, &mut port)
        .unwrap_or_else(|error| panic!("terminal publishes: {error:?}"));

    let TransactionSendTerminal::FailedHealthy {
        epoch: terminal_epoch,
        send_id: terminal_send_id,
        failure,
    } = observer
        .wait()
        .unwrap_or_else(|error| panic!("healthy terminal: {error:?}"))
    else {
        panic!("local rejection retains a healthy terminal");
    };
    assert_eq!((terminal_epoch, terminal_send_id), (epoch, send_id));
    assert_eq!(
        failure.kind(),
        TransactionSendFailureKind::Enrollment(
            crate::transaction::partition_enrollment::TransactionPartitionEnrollmentFailureKind::InvalidTarget
        )
    );
    assert_eq!(failure.delivery(), DeliveryStatus::NotSent);
    assert!(
        aggregate.host.commit(epoch, deadline(52)).is_ok(),
        "healthy local failure releases commit immediately"
    );
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("driver shuts down: {error:?}"));
}

#[test]
fn materialization_failure_releases_sequence_and_lifecycle_healthy() {
    let mut aggregate = FakeAggregate::new();
    let epoch = aggregate.epoch;
    let mut owner = aggregate.send_owner(CompressionPolicy::Snappy);
    let accepted = owner
        .try_send_with(&mut aggregate, request(epoch, "orders", 1))
        .unwrap_or_else(|error| panic!("send is accepted: {error:?}"));
    aggregate.enrolled();
    let mut driver = driver();
    let mut port = FakeProducePort::success(&aggregate, accepted.send_id());

    owner
        .turn_with(&mut aggregate, Moment::from_tick(1), &driver, &mut port)
        .unwrap_or_else(|error| panic!("enrollment terminal is consumed: {error:?}"));
    owner
        .turn_with(&mut aggregate, Moment::from_tick(2), &driver, &mut port)
        .unwrap_or_else(|error| panic!("materialization failure settles: {error:?}"));
    owner
        .turn_with(&mut aggregate, Moment::from_tick(3), &driver, &mut port)
        .unwrap_or_else(|error| panic!("terminal publishes: {error:?}"));
    let TransactionSendTerminal::FailedHealthy { failure, .. } = accepted
        .into_observer()
        .wait()
        .unwrap_or_else(|error| panic!("healthy terminal: {error:?}"))
    else {
        panic!("materialization failure is terminal");
    };
    assert_eq!(failure.kind(), TransactionSendFailureKind::Materialization);
    assert_eq!(failure.delivery(), DeliveryStatus::NotSent);
    assert!(aggregate.host.commit(epoch, deadline(53)).is_ok());

    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("driver shuts down: {error:?}"));
}
