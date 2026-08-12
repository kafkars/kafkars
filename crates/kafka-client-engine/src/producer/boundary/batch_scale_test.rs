//! Benchmark-shaped prefix admission across many explicit partitions.

use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use kafka_client_core::{ByteCount, ProducerBatchPolicy};

use super::{ProducerHandle, ProducerSendOptions, PublicProducerRecord};
use crate::{
    clock::MonotonicClock,
    producer::{
        host_limits_test::{start, valid_limits},
        ingress::{CountingWake, ProducerShardOwner},
    },
};

#[test]
fn one_public_batch_admits_256_records_across_twelve_partitions() {
    let mut limits = valid_limits();
    limits.retained_bytes = 64 * 1024 * 1024;
    limits.completion_capacity = 4_096;
    limits.record_capacity = 4_096;
    limits.batch_capacity = 4_096;
    limits.timer_capacity = 4_096;
    limits.encoded_byte_capacity = 64 * 1024 * 1024;
    limits.max_wire_batch_bytes = 65_536;
    limits.max_request_bytes = 1024 * 1024;
    limits.batch_policy = ProducerBatchPolicy::try_new(256, ByteCount::new(65_536), 5_000_000)
        .unwrap_or_else(|error| panic!("benchmark batch policy: {error:?}"));
    let wake = Arc::new(CountingWake::default());
    let owner = ProducerShardOwner::new(start(limits), Arc::clone(&wake));
    let handle = ProducerHandle::from_port(
        owner.admission_port(),
        Arc::new(MonotonicClock::new()),
        limits.record_capacity,
        Arc::new(()),
    );
    let capture = handle
        .capture_batch(ProducerSendOptions::new(Duration::from_secs(60)))
        .unwrap_or_else(|error| panic!("benchmark batch capture: {error}"));
    let records = (0..256)
        .map(|sequence| {
            PublicProducerRecord::to("orders")
                .partition(sequence % 12)
                .key(Bytes::copy_from_slice(&sequence.to_be_bytes()))
                .value(Bytes::from(vec![b'x'; 1_024]))
        })
        .collect();

    let (accepted, rejection) = handle
        .try_send_batch_captured(capture, records)
        .into_parts();

    assert!(rejection.is_none());
    assert_eq!(accepted.len(), 256);
    assert_eq!(wake.count(), 1);
    drop((accepted, owner));
}
