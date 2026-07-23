//! Engine-internal waiting-send boundary and ownership scenarios.

use std::{sync::Arc, task::Poll, time::Duration};

use bytes::Bytes;
use kafka_client_core::{ByteCount, ProducerBatchPolicy};

use super::{
    ProducerHandle, ProducerSendError, ProducerSendOptions, ProducerSendStartFailureKind,
    PublicProducerRecord as ProducerRecord,
};
use crate::{
    clock::MonotonicClock,
    producer::{
        ProducerHostLimits,
        host_limits_test::{start, valid_limits},
        ingress::{CountingWake as ShardWake, ProducerShardOwner},
        pending::test_support::{CountingWake as ObserverWake, poll_send},
    },
};

#[test]
fn internal_send_reports_validation_as_recordless_start_failure() {
    let (_owner, handle, wake) = setup(valid_limits());
    let result = handle
        .send(
            ProducerRecord::to("orders").value(Bytes::from_static(b"value")),
            ProducerSendOptions::new(Duration::from_secs(1)),
        )
        .wait();
    let Err(ProducerSendError::Start(failure)) = result else {
        panic!("missing partition should be a start failure")
    };

    assert_eq!(
        failure.kind(),
        ProducerSendStartFailureKind::MissingExplicitPartition
    );
    assert_eq!(wake.count(), 0);
}

#[test]
fn internal_send_registers_once_when_immediate_capacity_is_full() {
    let (owner, handle, wake) = setup(single_accepted_limits());
    let first = handle
        .try_send(
            record("first"),
            ProducerSendOptions::new(Duration::from_secs(1)),
        )
        .unwrap_or_else(|error| panic!("first record should admit: {error}"));
    let mut waiting = handle.send(
        record("second"),
        ProducerSendOptions::new(Duration::from_secs(1)),
    );

    assert_eq!(poll_send(&mut waiting, ObserverWake::new()), Poll::Pending);
    let stats = owner
        .try_data()
        .unwrap_or_else(|error| panic!("test should own shard: {error:?}"))
        .shard_stats();
    assert_eq!(stats.host.core_completion_slots, 1);
    assert_eq!(stats.pending.records, 1);
    assert_eq!(stats.pending.notification_permits, 1);
    assert_eq!(wake.count(), 2);
    drop((first, waiting));
}

fn setup(limits: ProducerHostLimits) -> (ProducerShardOwner, ProducerHandle, Arc<ShardWake>) {
    let wake = Arc::new(ShardWake::default());
    let owner = ProducerShardOwner::new(start(limits), Arc::clone(&wake));
    let handle = ProducerHandle::from_port(
        owner.admission_port(),
        Arc::new(MonotonicClock::new()),
        Arc::new(()),
    );
    (owner, handle, wake)
}

fn single_accepted_limits() -> ProducerHostLimits {
    let mut limits = valid_limits();
    limits.completion_capacity = 1;
    limits.record_capacity = 1;
    limits.batch_capacity = 1;
    limits.timer_capacity = 1;
    limits.notification_capacity = 1 + limits.pending_record_capacity;
    limits.batch_policy = ProducerBatchPolicy::try_new(1, ByteCount::new(64), 100)
        .unwrap_or_else(|error| panic!("single-record policy should validate: {error:?}"));
    limits
}

fn record(topic: &str) -> ProducerRecord {
    ProducerRecord::to(topic)
        .partition(0)
        .value(Bytes::from_static(b"value"))
}
