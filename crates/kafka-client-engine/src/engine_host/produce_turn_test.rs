//! Scenarios for lock-safe bounded producer and driver turns.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use kafka_client_core::{BatchExecutionGeneration, BatchExecutionId, BatchId, Deadline, Moment};

use crate::{
    EngineConfig,
    clock::OperationDeadline,
    driver::{DriverOwner, TrackedProduceCalls},
    producer::{
        host_limits_test::{start, valid_limits},
        ingress::{CountingWake, ProducerShardOwner},
        materialization::{MaterializationBatch, MaterializationRecord},
    },
    protocol::produce::materialize_explicit_produce_batch,
};

use super::produce_turn::apply_completions;

#[test]
fn completion_polling_waits_without_consuming_when_the_shard_is_contended() {
    let producer =
        ProducerShardOwner::new(start(valid_limits()), Arc::new(CountingWake::default()));
    let mut driver = driver();
    let mut calls = TrackedProduceCalls::new(1);
    calls
        .try_reserve()
        .unwrap_or_else(|| panic!("tracked-call capacity"))
        .submit(
            &driver,
            BatchExecutionId::new(BatchId::from_raw(1), BatchExecutionGeneration::initial()),
            OperationDeadline::from_parts_for_test(
                Deadline::from_tick(50_000_000),
                Instant::now() + Duration::from_millis(50),
            ),
            materialized(),
            Moment::from_tick(0),
        )
        .unwrap_or_else(|error| panic!("tracked Produce admission: {error}"));
    for turn in 0..64 {
        driver
            .turn(Duration::from_millis(10))
            .unwrap_or_else(|error| panic!("driver turn {turn}: {error}"));
    }
    let guard = producer
        .try_data()
        .unwrap_or_else(|error| panic!("acquire producer shard: {error:?}"));

    let mut identity_calls = crate::driver::TrackedProducerIdentityCalls::new();
    let mut partitioning_call = None;
    let progress = apply_completions(
        &driver,
        &producer,
        &mut identity_calls,
        &mut partitioning_call,
        &mut calls,
        Moment::from_tick(1),
    )
    .unwrap_or_else(|error| panic!("contended completion turn: {error}"));

    assert!(!progress);
    drop(guard);
    assert!(
        calls
            .poll_next_ready(Moment::from_tick(1))
            .unwrap_or_else(|error| panic!("ready completion retained: {error}"))
            .is_some()
    );
    calls.discard_settled();
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("bounded driver shutdown: {error}"));
}

fn materialized() -> crate::protocol::produce::MaterializedProduce {
    let batch = MaterializationBatch::try_for_test(
        "orders",
        0,
        vec![MaterializationRecord::new(
            1,
            None,
            Some(Bytes::from_static(b"value")),
            Vec::new(),
        )],
        1_024,
    )
    .unwrap_or_else(|| panic!("test materialization batch must be representable"));
    materialize_explicit_produce_batch(batch)
        .unwrap_or_else(|error| panic!("materialize Produce request: {error}"))
}

fn driver() -> DriverOwner {
    DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("build embedded driver: {error}"))
}
