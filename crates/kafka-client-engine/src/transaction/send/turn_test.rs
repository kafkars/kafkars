//! Successful Produce flow, original deadline, and local submit-failure tests.

use std::time::Duration;

use kafka_client_core::{CompressionPolicy, DeliveryStatus, Moment, ProducerAttemptFailureKind};

use super::{
    TransactionSendFailureKind, TransactionSendOwner, TransactionSendTerminal,
    test_support::{FakeAggregate, FakeProducePort, driver, local_submit_failure, request},
};

#[test]
fn success_preserves_deadline_and_discards_evidence_only_after_settlement() {
    let mut aggregate = FakeAggregate::new();
    let epoch = aggregate.epoch;
    let mut owner = aggregate.send_owner(CompressionPolicy::Snappy);
    let first_request = request(epoch, "orders", 1_024);
    let original_deadline = first_request.deadline();
    let accepted = owner
        .try_send_with(&mut aggregate, first_request)
        .unwrap_or_else(|error| panic!("send is accepted: {error:?}"));
    let send_id = accepted.send_id();
    let observer = accepted.into_observer();
    aggregate.enrolled();
    let mut driver = driver();
    let mut port = FakeProducePort::success(&aggregate, send_id);

    drive(&mut owner, &mut aggregate, &driver, &mut port, 4);
    assert_eq!(port.observed_deadline, Some(original_deadline));
    assert_eq!(port.observed_transactional_id.as_deref(), Some("writer"));
    assert!(
        aggregate
            .log
            .lock()
            .unwrap_or_else(|error| panic!("log: {error:?}"))
            .is_empty(),
        "accepted evidence is retained before aggregate settlement"
    );

    drive(&mut owner, &mut aggregate, &driver, &mut port, 1);
    assert_eq!(
        aggregate
            .log
            .lock()
            .unwrap_or_else(|error| panic!("log: {error:?}"))
            .as_slice(),
        &["settle", "discard"]
    );
    drive(&mut owner, &mut aggregate, &driver, &mut port, 1);
    let blocked = request(epoch, "payments", 1_024);
    let Err(failure) = owner.try_send_with(&mut aggregate, blocked) else {
        panic!("unobserved terminal unexpectedly released the fixed send slot");
    };
    assert_eq!(
        failure.kind(),
        super::TransactionSendAdmissionFailureKind::Busy
    );
    assert!(matches!(
        observer.wait(),
        Ok(TransactionSendTerminal::Succeeded {
            epoch: terminal_epoch,
            send_id: terminal_send_id,
            success,
            ..
        }) if terminal_epoch == epoch
            && terminal_send_id == send_id
            && success == kafka_client_core::ProducerBatchSuccess::new(42, None, None)
    ));
    drop(
        owner
            .try_send_with(&mut aggregate, request(epoch, "payments", 1_024))
            .unwrap_or_else(|error| {
                panic!("terminal consumption makes sequential admission reusable: {error:?}")
            })
            .into_observer(),
    );

    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("driver shuts down: {error:?}"));
}

#[test]
fn local_produce_submission_failure_settles_failed_healthy() {
    let mut aggregate = FakeAggregate::new();
    let epoch = aggregate.epoch;
    let mut owner = aggregate.send_owner(CompressionPolicy::None);
    let accepted = owner
        .try_send_with(&mut aggregate, request(epoch, "orders", 1_024))
        .unwrap_or_else(|error| panic!("send is accepted: {error:?}"));
    let send_id = accepted.send_id();
    let observer = accepted.into_observer();
    aggregate.enrolled();
    let mut driver = driver();
    let mut port = FakeProducePort::success(&aggregate, send_id);
    port.submit_failure = Some(local_submit_failure());

    drive(&mut owner, &mut aggregate, &driver, &mut port, 4);
    let Ok(TransactionSendTerminal::FailedHealthy { failure, .. }) = observer.wait() else {
        panic!("local submission failure is terminal");
    };
    assert_eq!(
        failure.kind(),
        TransactionSendFailureKind::ProduceSubmission(ProducerAttemptFailureKind::LocalCapacity)
    );
    assert_eq!(failure.delivery(), DeliveryStatus::NotSent);

    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("driver shuts down: {error:?}"));
}

fn drive(
    owner: &mut TransactionSendOwner,
    aggregate: &mut FakeAggregate,
    driver: &crate::driver::DriverOwner,
    port: &mut FakeProducePort,
    turns: u64,
) {
    for tick in 1..=turns {
        owner
            .turn_with(aggregate, Moment::from_tick(tick), driver, port)
            .unwrap_or_else(|error| panic!("send turn: {error:?}"));
    }
}
