//! Scenarios proving retries retain one core-owned identity and sequence lease.

use kafka_client_core::{
    BatchExecutionGeneration, BatchExecutionId, BatchId, ByteCount, Deadline, ExplicitRecord,
    Moment, PartitionIndex, PayloadId, ProducerAttemptFailureKind, ProducerBatchPolicy,
    ProducerEffect, ProducerInput, ProducerRetryPolicy, TopicId,
};

use crate::ProducerScenario;

#[test]
fn definitely_unsent_retry_reuses_identity_and_base_sequence() {
    let batch_policy = ProducerBatchPolicy::try_new(1, ByteCount::new(1_024), 10)
        .unwrap_or_else(|error| panic!("batch policy must be valid: {error}"));
    let retry_policy = ProducerRetryPolicy::try_fixed(1, 5)
        .unwrap_or_else(|error| panic!("retry policy must be valid: {error}"));
    let mut scenario = ProducerScenario::with_batch_and_retry_policy(
        ByteCount::new(128),
        1,
        batch_policy,
        retry_policy,
    );
    let payload_id = PayloadId::from_raw(1);
    scenario
        .retain_payload(payload_id, ByteCount::new(8))
        .unwrap_or_else(|error| panic!("payload retention failed: {error}"));
    let admitted = scenario
        .step(ProducerInput::AdmitExplicit {
            now: Moment::from_tick(0),
            deadline: Deadline::from_tick(20),
            record: ExplicitRecord::new(
                payload_id,
                TopicId::from_raw(2),
                PartitionIndex::from_raw(3),
                ByteCount::new(8),
            ),
        })
        .unwrap_or_else(|error| panic!("admission failed: {error}"));
    let (operation_id, batch_id) = admitted
        .effects()
        .iter()
        .find_map(|effect| match effect {
            ProducerEffect::AccumulateExplicit {
                operation_id,
                batch_id,
                ..
            } => Some((*operation_id, *batch_id)),
            _ => None,
        })
        .unwrap_or_else(|| panic!("admission must identify one batch"));
    scenario
        .step(ProducerInput::RecordAccumulated {
            operation_id,
            batch_id,
            accumulator_bytes: ByteCount::new(8),
            now: Moment::from_tick(0),
        })
        .unwrap_or_else(|error| panic!("accumulation failed: {error}"));
    let first = execution(batch_id, 1);
    scenario
        .step(ProducerInput::BatchMaterialized {
            execution: first,
            now: Moment::from_tick(0),
        })
        .unwrap_or_else(|error| panic!("materialization failed: {error}"));
    scenario
        .step(ProducerInput::DriverRejected {
            execution: first,
            now: Moment::from_tick(1),
            failure: ProducerAttemptFailureKind::ConnectionUnavailable,
        })
        .unwrap_or_else(|error| panic!("definitely-unsent failure failed: {error}"));
    scenario
        .advance(6)
        .unwrap_or_else(|error| panic!("retry timer failed: {error}"));

    let materializations = scenario
        .effect_trace()
        .iter()
        .filter_map(|effect| match effect {
            ProducerEffect::MaterializeBatch {
                execution,
                identity,
                sequence,
                ..
            } => Some((*execution, *identity, *sequence)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(materializations.len(), 2);
    assert_eq!(materializations[0].0, first);
    assert_eq!(materializations[1].0, execution(batch_id, 2));
    assert_eq!(materializations[1].1, materializations[0].1);
    assert_eq!(materializations[1].2, materializations[0].2);
}

fn execution(batch_id: BatchId, generation: u64) -> BatchExecutionId {
    let generation = BatchExecutionGeneration::try_from_raw(generation)
        .unwrap_or_else(|| panic!("test generation must be nonzero"));
    BatchExecutionId::new(batch_id, generation)
}
