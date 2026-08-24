//! Multi-record sequence, Produce, metadata, and batch-wide certainty scenarios.

use std::time::Duration;

use kafka_client_core::{
    CompressionPolicy, DeliveryStatus, Moment, ProducerAttemptFailureKind, ProducerBatchSuccess,
};

use crate::driver::transaction_produce::{
    TransactionProduceFailureKind, TransactionProduceTerminalFact,
};

use super::{
    TransactionSendFailureKind, TransactionSendOwner, TransactionSendTerminal,
    test_support::{FakeAggregate, FakeProducePort, batch_request, driver, produce_failure},
};

#[test]
fn three_records_use_one_contiguous_sequence_one_produce_and_exact_offset_range() {
    let mut aggregate = FakeAggregate::new();
    let epoch = aggregate.epoch;
    let mut owner = aggregate.send_owner(CompressionPolicy::None);
    let accepted = owner
        .try_send_with(&mut aggregate, batch_request(epoch, "orders", 4_096))
        .unwrap_or_else(|error| panic!("three-record batch is accepted: {error:?}"));
    let send_id = accepted.send_id();
    let observer = accepted.into_observer();
    aggregate.enrolled();
    let mut driver = driver();
    let mut port = FakeProducePort::success(&aggregate, send_id);

    drive(&mut owner, &mut aggregate, &driver, &mut port, 6);

    assert_eq!(aggregate.prepared_identities.len(), 1);
    let sequence = aggregate.prepared_identities[0].sequence();
    assert_eq!(sequence.base_sequence(), 0);
    assert_eq!(sequence.record_count(), 3);
    assert_eq!(port.submit_count, 1);
    assert_eq!(port.observed_records.len(), 1);
    assert!(!port.observed_records[0].is_empty());
    assert!(matches!(
        observer.wait(),
        Ok(TransactionSendTerminal::Succeeded {
            epoch: terminal_epoch,
            send_id: terminal_send,
            success,
            last_offset: 44,
            ..
        }) if terminal_epoch == epoch
            && terminal_send == send_id
            && success.base_offset() == 42
    ));
    shutdown(&mut driver);
}

#[test]
fn impossible_batch_offset_range_is_one_fatal_possibly_sent_terminal() {
    let mut aggregate = FakeAggregate::new();
    let epoch = aggregate.epoch;
    let mut owner = aggregate.send_owner(CompressionPolicy::None);
    let accepted = owner
        .try_send_with(&mut aggregate, batch_request(epoch, "orders", 4_096))
        .unwrap_or_else(|error| panic!("three-record batch is accepted: {error:?}"));
    let send_id = accepted.send_id();
    let observer = accepted.into_observer();
    aggregate.enrolled();
    let mut driver = driver();
    let mut port = FakeProducePort::success(&aggregate, send_id);
    port.fact = Some(TransactionProduceTerminalFact::Succeeded {
        epoch,
        send_id,
        success: ProducerBatchSuccess::new(i64::MAX, None, None),
    });

    drive(&mut owner, &mut aggregate, &driver, &mut port, 6);

    assert_eq!(port.submit_count, 1);
    assert!(matches!(
        observer.wait(),
        Ok(TransactionSendTerminal::Fatal { failure, .. })
            if failure.kind() == TransactionSendFailureKind::InvalidResponse
                && failure.delivery() == DeliveryStatus::PossiblySent
    ));
    shutdown(&mut driver);
}

#[test]
fn ordinary_produce_failure_is_one_batch_wide_certainty_and_consequence() {
    let mut aggregate = FakeAggregate::new();
    let epoch = aggregate.epoch;
    let mut owner = aggregate.send_owner(CompressionPolicy::None);
    let accepted = owner
        .try_send_with(&mut aggregate, batch_request(epoch, "orders", 4_096))
        .unwrap_or_else(|error| panic!("three-record batch is accepted: {error:?}"));
    let send_id = accepted.send_id();
    let observer = accepted.into_observer();
    aggregate.enrolled();
    let mut driver = driver();
    let mut port = FakeProducePort::success(&aggregate, send_id);
    port.fact = Some(TransactionProduceTerminalFact::AbortRequired {
        epoch,
        send_id,
        failure: produce_failure(
            TransactionProduceFailureKind::Driver(ProducerAttemptFailureKind::Permanent),
            DeliveryStatus::PossiblySent,
        ),
    });

    drive(&mut owner, &mut aggregate, &driver, &mut port, 6);

    assert_eq!(aggregate.prepared_identities.len(), 1);
    assert_eq!(
        aggregate.prepared_identities[0].sequence().record_count(),
        3
    );
    assert_eq!(port.submit_count, 1);
    assert!(matches!(
        observer.wait(),
        Ok(TransactionSendTerminal::AbortRequired { failure, .. })
            if failure.kind()
                == TransactionSendFailureKind::Produce(
                    TransactionProduceFailureKind::Driver(ProducerAttemptFailureKind::Permanent)
                )
                && failure.delivery() == DeliveryStatus::PossiblySent
    ));
    shutdown(&mut driver);
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
            .unwrap_or_else(|error| panic!("batch send turn: {error:?}"));
    }
}

fn shutdown(driver: &mut crate::driver::DriverOwner) {
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("driver shuts down: {error:?}"));
}
