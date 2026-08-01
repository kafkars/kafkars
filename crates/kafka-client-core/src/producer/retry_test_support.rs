//! Shared deterministic setup for sibling producer retry scenarios.

use crate::{
    BatchExecutionGeneration, BatchExecutionId, BatchId, BatchTimerGeneration, ByteCount, Deadline,
    ExplicitRecord, Moment, OperationId, PartitionIndex, PayloadId, ProducerBatchPolicy,
    ProducerEffect, ProducerInput, ProducerMachine, ProducerRetryPolicy, TopicId,
};

pub(in crate::producer) const RETAINED: ByteCount = ByteCount::new(8);

pub(crate) fn submitted(
    retries: u32,
    backoff: u64,
    deadline: u64,
) -> (ProducerMachine, OperationId, BatchExecutionId) {
    let retry = ProducerRetryPolicy::try_fixed(retries, backoff)
        .unwrap_or_else(|error| panic!("retry policy failed: {error}"));
    let mut producer = ProducerMachine::with_batch_and_retry_policy(
        ByteCount::new(64),
        1,
        crate::ProducerBatchPolicy::single_record(),
        retry,
    );
    producer.install_identity_for_test();
    let admitted = producer
        .apply(ProducerInput::AdmitExplicit {
            now: Moment::from_tick(0),
            deadline: Deadline::from_tick(deadline),
            record: ExplicitRecord::new(
                PayloadId::from_raw(1),
                TopicId::from_raw(1),
                PartitionIndex::from_raw(0),
                RETAINED,
            ),
        })
        .unwrap_or_else(|error| panic!("admission failed: {error}"));
    let Some(ProducerEffect::AccumulateExplicit {
        operation_id,
        batch_id,
        ..
    }) = admitted.effects().first()
    else {
        panic!("missing accumulation effect")
    };
    let operation_id = *operation_id;
    let execution = BatchExecutionId::new(*batch_id, BatchExecutionGeneration::initial());
    producer
        .apply(ProducerInput::RecordAccumulated {
            operation_id,
            batch_id: *batch_id,
            accumulator_bytes: RETAINED,
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("accumulation failed: {error}"));
    materialize_and_submit(&mut producer, execution, 1);
    (producer, operation_id, execution)
}

pub(in crate::producer) fn submitted_pair(
    retries: u32,
    backoff: u64,
    deadline: u64,
) -> (ProducerMachine, OperationId, OperationId, BatchExecutionId) {
    let batch_policy = ProducerBatchPolicy::try_new(2, ByteCount::new(64), 20)
        .unwrap_or_else(|error| panic!("batch policy failed: {error}"));
    let retry_policy = ProducerRetryPolicy::try_fixed(retries, backoff)
        .unwrap_or_else(|error| panic!("retry policy failed: {error}"));
    let mut producer = ProducerMachine::with_batch_and_retry_policy(
        ByteCount::new(64),
        2,
        batch_policy,
        retry_policy,
    );
    producer.install_identity_for_test();
    let (first, batch_id, _) = admit_and_accumulate(&mut producer, 1, 0, deadline);
    let (second, same_batch, _) = admit_and_accumulate(&mut producer, 2, 0, deadline);
    assert_eq!(same_batch, batch_id);
    let execution = BatchExecutionId::new(batch_id, BatchExecutionGeneration::initial());
    materialize_and_submit(&mut producer, execution, 1);
    (producer, first, second, execution)
}

pub(in crate::producer) fn fire_retry(
    producer: &mut ProducerMachine,
    execution: BatchExecutionId,
    timer_generation: u64,
    now: u64,
) -> crate::ProducerTransition {
    producer
        .apply(ProducerInput::BatchTimerFired {
            batch_id: execution.batch_id(),
            generation: BatchTimerGeneration::from_raw(timer_generation),
            now: Moment::from_tick(now),
        })
        .unwrap_or_else(|error| panic!("retry timer failed: {error}"))
}

pub(in crate::producer) fn materialize_and_submit(
    producer: &mut ProducerMachine,
    execution: BatchExecutionId,
    now: u64,
) {
    producer
        .apply(ProducerInput::BatchMaterialized {
            execution,
            now: Moment::from_tick(now),
        })
        .unwrap_or_else(|error| panic!("materialization failed: {error}"));
    producer
        .apply(ProducerInput::DriverAccepted { execution })
        .unwrap_or_else(|error| panic!("driver acceptance failed: {error}"));
}

pub(crate) fn transient_failure(
    producer: &mut ProducerMachine,
    execution: BatchExecutionId,
    now: u64,
) -> crate::ProducerTransition {
    producer
        .apply(ProducerInput::TransportFailed {
            execution,
            now: Moment::from_tick(now),
            failure: crate::ProducerAttemptFailureKind::ConnectionUnavailable,
            delivery: crate::DeliveryStatus::NotSent,
            route_refreshed: false,
        })
        .unwrap_or_else(|error| panic!("transient failure failed: {error}"))
}

pub(in crate::producer) fn next(execution: BatchExecutionId) -> BatchExecutionId {
    let generation = execution
        .generation()
        .get()
        .checked_add(1)
        .and_then(BatchExecutionGeneration::try_from_raw)
        .unwrap_or_else(|| panic!("test execution generation must advance"));
    BatchExecutionId::new(execution.batch_id(), generation)
}

pub(in crate::producer) fn has_retry(effects: &[ProducerEffect]) -> bool {
    effects
        .iter()
        .any(|effect| matches!(effect, ProducerEffect::RetryBatchExecution { .. }))
}

pub(in crate::producer) fn admit_and_accumulate(
    producer: &mut ProducerMachine,
    payload: u64,
    now: u64,
    deadline: u64,
) -> (OperationId, BatchId, crate::ProducerTransition) {
    let admitted = producer
        .apply(ProducerInput::AdmitExplicit {
            now: Moment::from_tick(now),
            deadline: Deadline::from_tick(deadline),
            record: ExplicitRecord::new(
                PayloadId::from_raw(payload),
                TopicId::from_raw(1),
                PartitionIndex::from_raw(0),
                RETAINED,
            ),
        })
        .unwrap_or_else(|error| panic!("admission failed: {error}"));
    let Some(ProducerEffect::AccumulateExplicit {
        operation_id,
        batch_id,
        ..
    }) = admitted.effects().first()
    else {
        panic!("missing accumulation effect")
    };
    let result = (*operation_id, *batch_id);
    let accumulated = producer
        .apply(ProducerInput::RecordAccumulated {
            operation_id: result.0,
            batch_id: result.1,
            accumulator_bytes: RETAINED,
            now: Moment::from_tick(now.saturating_add(1)),
        })
        .unwrap_or_else(|error| panic!("accumulation failed: {error}"));
    (result.0, result.1, accumulated)
}
