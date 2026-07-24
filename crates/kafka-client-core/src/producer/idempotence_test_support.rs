//! Shared deterministic setup for producer identity and sequence scenarios.

use crate::{
    BatchExecutionGeneration, BatchExecutionId, BatchId, ByteCount, Deadline, ExplicitRecord,
    Moment, OperationId, PartitionIndex, PayloadId, ProducerEffect, ProducerIdentityGeneration,
    ProducerInput, ProducerMachine, TopicId,
};

pub(in crate::producer) const TOPIC: TopicId = TopicId::from_raw(4);
pub(in crate::producer) const BYTES: ByteCount = ByteCount::new(8);

pub(in crate::producer) fn acquire(
    producer: &mut ProducerMachine,
    now: u64,
) -> crate::ProducerTransition {
    producer
        .apply(ProducerInput::ProducerIdentityAcquired {
            generation: ProducerIdentityGeneration::initial(),
            producer_id: 11,
            producer_epoch: 3,
            now: Moment::from_tick(now),
        })
        .unwrap_or_else(|error| panic!("identity acquisition failed: {error}"))
}

pub(in crate::producer) fn admit(
    producer: &mut ProducerMachine,
    payload: u64,
    partition: u32,
    deadline: u64,
) -> (OperationId, BatchId) {
    let admitted = producer
        .apply(ProducerInput::AdmitExplicit {
            now: Moment::from_tick(0),
            deadline: Deadline::from_tick(deadline),
            record: record(payload, partition),
        })
        .unwrap_or_else(|error| panic!("admission failed: {error}"));
    match admitted.effects().first() {
        Some(ProducerEffect::AccumulateExplicit {
            operation_id,
            batch_id,
            ..
        }) => (*operation_id, *batch_id),
        effect => panic!("unexpected admission effect: {effect:?}"),
    }
}

pub(in crate::producer) fn accumulate(
    producer: &mut ProducerMachine,
    operation_id: OperationId,
    batch_id: BatchId,
    now: u64,
) -> crate::ProducerTransition {
    producer
        .apply(ProducerInput::RecordAccumulated {
            operation_id,
            batch_id,
            accumulator_bytes: BYTES,
            now: Moment::from_tick(now),
        })
        .unwrap_or_else(|error| panic!("accumulation failed: {error}"))
}

pub(in crate::producer) fn submit(producer: &mut ProducerMachine, batch_id: BatchId) {
    producer
        .apply(ProducerInput::BatchMaterialized {
            execution: execution(batch_id),
            now: Moment::from_tick(2),
        })
        .unwrap_or_else(|error| panic!("materialization failed: {error}"));
    producer
        .apply(ProducerInput::DriverAccepted {
            execution: execution(batch_id),
        })
        .unwrap_or_else(|error| panic!("driver acceptance failed: {error}"));
}

pub(in crate::producer) const fn execution(batch_id: BatchId) -> BatchExecutionId {
    BatchExecutionId::new(batch_id, BatchExecutionGeneration::initial())
}

pub(in crate::producer) const fn record(payload: u64, partition: u32) -> ExplicitRecord {
    ExplicitRecord::new(
        PayloadId::from_raw(payload),
        TOPIC,
        PartitionIndex::from_raw(partition),
        BYTES,
    )
}
