//! Enrollment, Produce correlation, and exact shutdown recovery tests.

use std::time::Duration;

use kafka_client_core::{
    CompressionPolicy, DeliveryStatus, Moment, ProducerAttemptFailureKind, TransactionEpoch,
    TransactionSendId,
};

use crate::{
    driver::{
        DriverOwner,
        transaction_produce::{TransactionProduceFailureKind, TransactionProduceTerminalFact},
    },
    transaction::partition_enrollment::TransactionPartitionEnrollmentFailureKind,
};

use super::{
    TransactionSendFailureKind, TransactionSendOwner, TransactionSendTerminal,
    test_support::{FakeAggregate, FakeProducePort, driver, later_epoch, produce_failure, request},
};

#[test]
fn enrollment_abort_and_fatal_settle_without_produce_submission() {
    let mut driver = driver();

    let abort = enrollment_terminal(&driver, false);
    assert!(matches!(
        abort,
        TransactionSendTerminal::AbortRequired { failure, .. }
            if failure.kind()
                == TransactionSendFailureKind::Enrollment(
                    TransactionPartitionEnrollmentFailureKind::Transport
                )
                && failure.delivery() == DeliveryStatus::PossiblySent
    ));
    let fatal = enrollment_terminal(&driver, true);
    assert!(matches!(
        fatal,
        TransactionSendTerminal::Fatal { failure, .. }
            if failure.kind()
                == TransactionSendFailureKind::Enrollment(
                    TransactionPartitionEnrollmentFailureKind::Broker {
                        code: 90,
                        fenced: true,
                    }
                )
    ));

    shutdown(&mut driver);
}

#[test]
fn produce_abort_and_fatal_map_to_closed_send_terminals() {
    let mut driver = driver();
    let abort = produce_terminal(&driver, |epoch, send_id| {
        TransactionProduceTerminalFact::AbortRequired {
            epoch,
            send_id,
            failure: produce_failure(
                TransactionProduceFailureKind::Driver(ProducerAttemptFailureKind::Permanent),
                DeliveryStatus::NotSent,
            ),
        }
    });
    assert!(matches!(
        abort,
        TransactionSendTerminal::AbortRequired { failure, .. }
            if failure.kind()
                == TransactionSendFailureKind::Produce(
                    TransactionProduceFailureKind::Driver(
                        ProducerAttemptFailureKind::Permanent
                    )
                )
    ));

    let fatal = produce_terminal(&driver, |epoch, send_id| {
        TransactionProduceTerminalFact::Fatal {
            epoch,
            send_id,
            failure: produce_failure(
                TransactionProduceFailureKind::Driver(ProducerAttemptFailureKind::Permanent),
                DeliveryStatus::PossiblySent,
            ),
        }
    });
    assert!(matches!(
        fatal,
        TransactionSendTerminal::Fatal { failure, .. }
            if failure.delivery() == DeliveryStatus::PossiblySent
    ));
    shutdown(&mut driver);
}

#[test]
fn stale_epoch_and_send_id_are_fatal_correlation_failures() {
    let mut driver = driver();
    let stale_epoch = produce_terminal(&driver, |_epoch, send_id| {
        TransactionProduceTerminalFact::Succeeded {
            epoch: later_epoch(),
            send_id,
            success: kafka_client_core::ProducerBatchSuccess::new(1, None, None),
        }
    });
    let stale_send = produce_terminal(&driver, |epoch, send_id| {
        TransactionProduceTerminalFact::Succeeded {
            epoch,
            send_id: TransactionSendId::from_raw(send_id.get() + 1),
            success: kafka_client_core::ProducerBatchSuccess::new(1, None, None),
        }
    });
    for terminal in [stale_epoch, stale_send] {
        assert!(matches!(
            terminal,
            TransactionSendTerminal::Fatal { failure, .. }
                if failure.kind() == TransactionSendFailureKind::Correlation
                    && failure.delivery() == DeliveryStatus::PossiblySent
        ));
    }
    shutdown(&mut driver);
}

#[test]
fn driver_shutdown_recovers_the_exact_producing_owner_before_discard() {
    let mut aggregate = FakeAggregate::new();
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
            TransactionProduceFailureKind::DriverShutdown,
            DeliveryStatus::PossiblySent,
        ),
    });
    drive(&mut owner, &mut aggregate, &driver, &mut port, 3);

    owner
        .recover_with(&mut aggregate)
        .unwrap_or_else(|error| panic!("producing call recovers retained evidence: {error:?}"));
    owner
        .publish_terminal_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recovered terminal publishes: {error:?}"));
    assert!(matches!(
        observer.wait(),
        Ok(TransactionSendTerminal::Fatal {
            epoch: terminal_epoch,
            send_id: terminal_send_id,
            failure,
        }) if terminal_epoch == epoch
            && terminal_send_id == send_id
            && failure.kind()
                == TransactionSendFailureKind::Produce(
                    TransactionProduceFailureKind::DriverShutdown
                )
    ));
    assert_eq!(
        aggregate
            .log
            .lock()
            .unwrap_or_else(|error| panic!("log: {error:?}"))
            .as_slice(),
        &["settle", "discard"]
    );
    shutdown(&mut driver);
}

fn enrollment_terminal(driver: &DriverOwner, fatal: bool) -> TransactionSendTerminal {
    let mut aggregate = FakeAggregate::new();
    let epoch = aggregate.epoch;
    let mut owner = aggregate.send_owner(CompressionPolicy::None);
    let accepted = owner
        .try_send_with(&mut aggregate, request(epoch, "orders", 1_024))
        .unwrap_or_else(|error| panic!("send accepted: {error:?}"));
    let send_id = accepted.send_id();
    let observer = accepted.into_observer();
    if fatal {
        aggregate.enrollment_fatal();
    } else {
        aggregate.enrollment_abort_required();
    }
    let mut port = FakeProducePort::success(&aggregate, send_id);
    drive(&mut owner, &mut aggregate, driver, &mut port, 2);
    assert!(port.observed_deadline.is_none());
    observer
        .wait()
        .unwrap_or_else(|error| panic!("enrollment terminal: {error:?}"))
}

fn produce_terminal(
    driver: &DriverOwner,
    fact: impl FnOnce(TransactionEpoch, TransactionSendId) -> TransactionProduceTerminalFact,
) -> TransactionSendTerminal {
    let mut aggregate = FakeAggregate::new();
    let epoch = aggregate.epoch;
    let mut owner = aggregate.send_owner(CompressionPolicy::None);
    let accepted = owner
        .try_send_with(&mut aggregate, request(epoch, "orders", 1_024))
        .unwrap_or_else(|error| panic!("send accepted: {error:?}"));
    let send_id = accepted.send_id();
    let observer = accepted.into_observer();
    aggregate.enrolled();
    let mut port = FakeProducePort::success(&aggregate, send_id);
    port.fact = Some(fact(epoch, send_id));
    drive(&mut owner, &mut aggregate, driver, &mut port, 6);
    observer
        .wait()
        .unwrap_or_else(|error| panic!("produce terminal: {error:?}"))
}

fn drive(
    owner: &mut TransactionSendOwner,
    aggregate: &mut FakeAggregate,
    driver: &DriverOwner,
    port: &mut FakeProducePort,
    turns: u64,
) {
    for tick in 1..=turns {
        owner
            .turn_with(aggregate, Moment::from_tick(tick), driver, port)
            .unwrap_or_else(|error| panic!("send turn: {error:?}"));
    }
}

fn shutdown(driver: &mut DriverOwner) {
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("driver shuts down: {error:?}"));
}
