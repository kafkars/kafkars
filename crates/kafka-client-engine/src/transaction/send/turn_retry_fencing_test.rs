//! Non-routing and foreign-attempt failures remain terminal without replacement.

use std::{num::NonZeroI16, time::Duration};

use kafka_client_core::{
    CompressionPolicy, DeliveryStatus, Moment, ProducerBrokerFailure, ProducerBrokerFailureKind,
    ProducerRetryPolicy, TransactionSendAttempt,
};

use crate::driver::transaction_produce::{
    TransactionProduceFailureKind, TransactionProduceRouteRefreshPoll,
    TransactionProduceTerminalFact,
};

use super::{
    TransactionSendFailureKind, TransactionSendTerminal,
    test_support::{FakeAggregate, FakeProducePort, driver, produce_failure, request},
};

#[test]
fn fencing_failure_never_enters_route_invalidation_or_replacement() {
    let policy = ProducerRetryPolicy::try_fixed(2, 10)
        .unwrap_or_else(|error| panic!("bounded retry policy: {error:?}"));
    let mut aggregate = FakeAggregate::with_retry_policy(policy);
    let epoch = aggregate.epoch;
    let mut owner = aggregate.send_owner(CompressionPolicy::None);
    let accepted = owner
        .try_send_with(&mut aggregate, request(epoch, "orders", 1_024))
        .unwrap_or_else(|error| panic!("send accepted: {error:?}"));
    let send_id = accepted.send_id();
    let observer = accepted.into_observer();
    aggregate.enrolled();
    let mut driver = driver();
    let mut port = FakeProducePort::success(&aggregate, send_id);
    port.fact = Some(TransactionProduceTerminalFact::Fatal {
        epoch,
        send_id,
        failure: produce_failure(
            TransactionProduceFailureKind::Broker(ProducerBrokerFailure::new(
                ProducerBrokerFailureKind::ProducerFenced,
                NonZeroI16::new(90).unwrap_or_else(|| panic!("nonzero broker code")),
            )),
            DeliveryStatus::PossiblySent,
        ),
    });
    drive(&mut owner, &mut aggregate, &driver, &mut port, 6);
    assert_eq!(port.submit_count, 1);
    assert!(
        port.route_refresh_polls
            .lock()
            .unwrap_or_else(|error| panic!("route script: {error:?}"))
            .is_empty()
    );
    assert!(matches!(
        observer.wait(),
        Ok(TransactionSendTerminal::Fatal { .. })
    ));
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("driver shuts down: {error:?}"));
}

#[test]
fn foreign_attempt_terminal_is_fatal_correlation_and_never_retried() {
    let policy = ProducerRetryPolicy::try_fixed(2, 10)
        .unwrap_or_else(|error| panic!("bounded retry policy: {error:?}"));
    let mut aggregate = FakeAggregate::with_retry_policy(policy);
    let epoch = aggregate.epoch;
    let mut owner = aggregate.send_owner(CompressionPolicy::None);
    let accepted = owner
        .try_send_with(&mut aggregate, request(epoch, "orders", 1_024))
        .unwrap_or_else(|error| panic!("send accepted: {error:?}"));
    let send_id = accepted.send_id();
    let observer = accepted.into_observer();
    aggregate.enrolled();
    let mut driver = driver();
    let mut port = FakeProducePort::success(&aggregate, send_id);
    port.fact = Some(routing_failure(epoch, send_id));
    port.replacement_fact = Some(TransactionProduceTerminalFact::Succeeded {
        epoch,
        send_id,
        success: kafka_client_core::ProducerBatchSuccess::new(42, None, None),
    });
    port.replacement_terminal_attempt = Some(TransactionSendAttempt::initial());
    port.route_refresh_polls
        .lock()
        .unwrap_or_else(|error| panic!("route poll script: {error:?}"))
        .push_back(TransactionProduceRouteRefreshPoll::Ready);

    for tick in 1..=6 {
        owner
            .turn_with(&mut aggregate, Moment::from_tick(tick), &driver, &mut port)
            .unwrap_or_else(|error| panic!("first attempt and invalidation: {error:?}"));
    }
    owner
        .turn_with(&mut aggregate, Moment::from_tick(15), &driver, &mut port)
        .unwrap_or_else(|error| panic!("replacement admission: {error:?}"));
    for tick in 16..=18 {
        owner
            .turn_with(&mut aggregate, Moment::from_tick(tick), &driver, &mut port)
            .unwrap_or_else(|error| panic!("foreign replacement terminal: {error:?}"));
    }
    assert_eq!(port.submit_count, 2);
    assert!(matches!(
        observer.wait(),
        Ok(TransactionSendTerminal::Fatal { failure, .. })
            if failure.kind() == TransactionSendFailureKind::Correlation
                && failure.delivery() == DeliveryStatus::PossiblySent
    ));
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("driver shuts down: {error:?}"));
}

fn routing_failure(
    epoch: kafka_client_core::TransactionEpoch,
    send_id: kafka_client_core::TransactionSendId,
) -> TransactionProduceTerminalFact {
    TransactionProduceTerminalFact::AbortRequired {
        epoch,
        send_id,
        failure: produce_failure(
            TransactionProduceFailureKind::Broker(ProducerBrokerFailure::new(
                ProducerBrokerFailureKind::Routing,
                NonZeroI16::new(6).unwrap_or_else(|| panic!("nonzero broker code")),
            )),
            DeliveryStatus::PossiblySent,
        ),
    }
}

fn drive(
    owner: &mut super::TransactionSendOwner,
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
