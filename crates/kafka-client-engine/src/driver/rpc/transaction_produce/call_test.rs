//! Exact submission correlation and definitely-unsent admission evidence.

use std::time::{Duration, Instant};

use bytes::Bytes;
use kafka_client_core::{
    Deadline, DeliveryStatus, Moment, OperationId, TransactionEndOutcome, TransactionEpoch,
    TransactionLifecycleEffect, TransactionLifecycleInput, TransactionLifecycleMachine,
    TransactionSendAttempt, TransactionSendId, TransactionalOwnerId,
};
use kafka_driver::SubmitError;

use crate::{EngineConfig, clock::OperationDeadline, protocol::produce::MaterializedProduce};

use super::call::TransactionProduceCall;

#[test]
fn only_capacity_full_invalidation_admission_is_retryable() {
    assert!(super::route_refresh::invalidation_rejection_is_retryable(
        &SubmitError::Full
    ));
    for permanent in [
        SubmitError::Closed,
        SubmitError::ForeignDriver,
        SubmitError::IdentityExhausted,
        SubmitError::Wake(std::io::Error::other("wake failed")),
    ] {
        assert!(!super::route_refresh::invalidation_rejection_is_retryable(
            &permanent
        ));
    }
}

#[test]
fn accepted_call_retains_exact_transaction_send_and_partition_correlation() {
    let mut driver = owner();
    let mut call = TransactionProduceCall::submit(
        &driver,
        epoch(4),
        TransactionSendId::from_raw(9),
        TransactionSendAttempt::initial(),
        "invoice-writer",
        &materialized("orders", 3),
        Moment::from_tick(10),
        deadline(),
    )
    .unwrap_or_else(|error| panic!("transactional Produce admission: {error}"));

    assert_eq!(call.epoch(), epoch(4));
    assert_eq!(call.send_id(), TransactionSendId::from_raw(9));
    assert_eq!(call.attempt(), TransactionSendAttempt::initial());
    assert_eq!(call.topic().as_ref(), "orders");
    assert_eq!(call.partition(), 3);
    assert!(call.try_terminal().is_none());

    drop(call);
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("bounded driver shutdown: {error}"));
}

#[test]
fn local_route_rejection_is_explicitly_definitely_unsent() {
    let mut driver = owner();
    let error = TransactionProduceCall::submit(
        &driver,
        epoch(5),
        TransactionSendId::from_raw(10),
        TransactionSendAttempt::initial(),
        "invoice-writer",
        &materialized("", 0),
        Moment::from_tick(10),
        deadline(),
    )
    .err()
    .unwrap_or_else(|| panic!("empty route topic must reject"));

    assert_eq!(error.epoch(), epoch(5));
    assert_eq!(error.send_id(), TransactionSendId::from_raw(10));
    assert_eq!(error.delivery(), DeliveryStatus::NotSent);
    assert_eq!(
        error.failure_kind(),
        kafka_client_core::ProducerAttemptFailureKind::Permanent
    );

    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|shutdown| panic!("bounded driver shutdown: {shutdown}"));
}

#[test]
fn driver_shutdown_recovery_is_correlated_and_fatal() {
    let mut driver = owner();
    let call = TransactionProduceCall::submit(
        &driver,
        epoch(6),
        TransactionSendId::from_raw(12),
        TransactionSendAttempt::initial(),
        "invoice-writer",
        &materialized("orders", 5),
        Moment::from_tick(10),
        deadline(),
    )
    .unwrap_or_else(|error| panic!("transactional Produce admission: {error}"));
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("bounded driver shutdown: {error}"));

    let terminal = call.recover_after_driver_shutdown();
    assert_eq!(terminal.epoch(), epoch(6));
    assert_eq!(terminal.send_id(), TransactionSendId::from_raw(12));
    assert_eq!(terminal.attempt(), TransactionSendAttempt::initial());
    assert!(matches!(
        terminal.fact(),
        super::model::TransactionProduceTerminalFact::Fatal { .. }
    ));
    terminal.discard();
}

fn materialized(topic: &str, partition: i32) -> MaterializedProduce {
    MaterializedProduce::from_encoded_test_parts(
        topic,
        partition,
        Bytes::from_static(b"transactional-record-batch"),
    )
}

fn deadline() -> OperationDeadline {
    OperationDeadline::from_parts_for_test(
        Deadline::from_tick(1_000_000_000),
        Instant::now() + Duration::from_secs(1),
    )
}

fn owner() -> crate::driver::DriverOwner {
    crate::driver::DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("build embedded driver owner: {error}"))
}

fn epoch(target: u64) -> TransactionEpoch {
    let owner = TransactionalOwnerId::from_raw(1);
    let mut machine = TransactionLifecycleMachine::new(owner);
    for current in 1..=target {
        let transition = machine
            .apply(owner, TransactionLifecycleInput::Begin)
            .unwrap_or_else(|error| panic!("begin transaction {current}: {error:?}"));
        let Some(TransactionLifecycleEffect::Began { epoch, .. }) = transition.into_effect() else {
            panic!("begin transaction {current} must allocate an epoch");
        };
        if current == target {
            return epoch;
        }
        machine
            .apply(
                owner,
                TransactionLifecycleInput::Abort {
                    epoch,
                    operation_id: OperationId::from_raw(current),
                },
            )
            .unwrap_or_else(|error| panic!("abort transaction {current}: {error:?}"));
        machine
            .apply(
                owner,
                TransactionLifecycleInput::EndSettled {
                    epoch,
                    outcome: TransactionEndOutcome::Succeeded,
                },
            )
            .unwrap_or_else(|error| panic!("settle transaction {current}: {error:?}"));
    }
    unreachable!("transaction epoch test scalar is positive")
}
