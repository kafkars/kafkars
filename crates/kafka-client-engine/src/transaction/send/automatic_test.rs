//! Automatic transactional partition selection and deadline scenarios.

use std::time::Duration;

use bytes::Bytes;
use kafka_client_core::{
    CompressionPolicy, Moment, PartitionIndex,
    partitioning::{
        AvailablePartition, PartitionCount, TopicMetadataGeneration, TopicPartitionSource,
    },
};

use super::{
    TransactionSendFailureKind, TransactionSendTerminal,
    partitioning::TransactionPartitioningFailure,
    test_support::{FakeAggregate, FakeProducePort, automatic_request, deadline, driver},
};

#[test]
fn keyed_send_uses_java_logical_domain_then_owns_the_exact_sequence() {
    let mut aggregate = FakeAggregate::new();
    let epoch = aggregate.epoch;
    let mut owner = aggregate.send_owner(CompressionPolicy::None);
    let accepted = owner
        .try_send_with(
            &mut aggregate,
            automatic_request(epoch, "orders", Some(Bytes::from_static(b"kafka")), 1_024),
        )
        .unwrap_or_else(|error| panic!("automatic send crosses lifecycle acceptance: {error:?}"));
    assert!(
        aggregate.host.commit(epoch, deadline(51)).is_err(),
        "metadata lookup remains fenced as an outstanding send"
    );

    owner
        .apply_partitioning_for_test(&topic_view(), &mut aggregate)
        .unwrap_or_else(|error| panic!("immutable topic view resolves: {error:?}"));
    aggregate.enrolled();
    let send_id = accepted.send_id();
    let observer = accepted.into_observer();
    let mut driver = driver();
    let mut port = FakeProducePort::success(&aggregate, send_id);
    for tick in 1..=6 {
        owner
            .turn_with(&mut aggregate, Moment::from_tick(tick), &driver, &mut port)
            .unwrap_or_else(|error| panic!("resolved send turn: {error:?}"));
    }

    assert!(matches!(
        observer.wait(),
        Ok(TransactionSendTerminal::Succeeded {
            partition,
            ..
        }) if partition == PartitionIndex::from_raw(4)
    ));
    assert!(aggregate.host.commit(epoch, deadline(52)).is_ok());
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("driver shuts down: {error:?}"));
}

#[test]
fn automatic_send_deadline_settles_not_sent_and_reopens_commit() {
    let mut aggregate = FakeAggregate::new();
    let epoch = aggregate.epoch;
    let mut owner = aggregate.send_owner(CompressionPolicy::None);
    let accepted = owner
        .try_send_with(
            &mut aggregate,
            automatic_request(epoch, "orders", None, 1_024),
        )
        .unwrap_or_else(|error| panic!("automatic send accepted: {error:?}"));
    let send_id = accepted.send_id();
    let observer = accepted.into_observer();
    let mut driver = driver();
    let mut port = FakeProducePort::success(&aggregate, send_id);

    owner
        .turn_with(&mut aggregate, Moment::from_tick(50), &driver, &mut port)
        .unwrap_or_else(|error| panic!("original deadline settles lookup: {error:?}"));
    owner
        .turn_with(&mut aggregate, Moment::from_tick(51), &driver, &mut port)
        .unwrap_or_else(|error| panic!("terminal publishes: {error:?}"));
    assert!(matches!(
        observer.wait(),
        Ok(TransactionSendTerminal::FailedHealthy { failure, .. })
            if failure.kind()
                == TransactionSendFailureKind::Partitioning(
                    TransactionPartitioningFailure::DeadlineElapsed
                )
    ));
    assert!(aggregate.host.commit(epoch, deadline(52)).is_ok());
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("driver shuts down: {error:?}"));
}

#[test]
fn unkeyed_send_advances_only_after_its_transactional_batch_seals() {
    let mut aggregate = FakeAggregate::new();
    let epoch = aggregate.epoch;
    let mut owner = aggregate.send_owner(CompressionPolicy::None);
    let first = owner
        .try_send_with(
            &mut aggregate,
            automatic_request(epoch, "orders", None, 1_024),
        )
        .unwrap_or_else(|error| panic!("first automatic send: {error:?}"));
    owner
        .apply_partitioning_for_test(&topic_view(), &mut aggregate)
        .unwrap_or_else(|error| panic!("first view: {error:?}"));
    assert_eq!(aggregate.captured_partition(), Some(1));
    aggregate.enrolled();
    let first_id = first.send_id();
    let first_observer = first.into_observer();
    let mut driver = driver();
    let mut first_port = FakeProducePort::success(&aggregate, first_id);
    for tick in 1..=6 {
        owner
            .turn_with(
                &mut aggregate,
                Moment::from_tick(tick),
                &driver,
                &mut first_port,
            )
            .unwrap_or_else(|error| panic!("first send turn: {error:?}"));
    }
    assert!(matches!(
        first_observer.wait(),
        Ok(TransactionSendTerminal::Succeeded { partition, .. })
            if partition == PartitionIndex::from_raw(1)
    ));
    owner
        .turn_with(
            &mut aggregate,
            Moment::from_tick(7),
            &driver,
            &mut first_port,
        )
        .unwrap_or_else(|error| panic!("first completion reclaims: {error:?}"));

    let second = owner
        .try_send_with(
            &mut aggregate,
            automatic_request(epoch, "orders", None, 1_024),
        )
        .unwrap_or_else(|error| panic!("second automatic send: {error:?}"));
    owner
        .apply_partitioning_for_test(&topic_view(), &mut aggregate)
        .unwrap_or_else(|error| panic!("second view: {error:?}"));
    assert_eq!(
        aggregate.captured_partition(),
        Some(7),
        "sealed singleton advances the per-topic sticky cursor"
    );
    drop(second.into_observer());
    owner
        .recover_with(&mut aggregate)
        .unwrap_or_else(|error| panic!("second send recovers before driver shutdown: {error:?}"));
    owner
        .publish_terminal_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("shutdown terminal publishes: {error:?}"));
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("driver shuts down: {error:?}"));
}

fn topic_view() -> TestTopicView {
    TestTopicView {
        available: vec![
            AvailablePartition::new(PartitionIndex::from_raw(1), None),
            AvailablePartition::new(PartitionIndex::from_raw(7), None),
        ],
    }
}

struct TestTopicView {
    available: Vec<AvailablePartition>,
}

impl TopicPartitionSource for TestTopicView {
    fn generation(&self) -> TopicMetadataGeneration {
        TopicMetadataGeneration::from_raw(21)
    }

    fn logical_count(&self) -> PartitionCount {
        PartitionCount::try_from_raw(12).unwrap_or_else(|| panic!("valid count"))
    }

    fn available_len(&self) -> usize {
        self.available.len()
    }

    fn available_at(&self, index: usize) -> Option<AvailablePartition> {
        self.available.get(index).copied()
    }
}
