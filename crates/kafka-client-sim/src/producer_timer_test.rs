//! Chronological virtual-time scenarios for producer deadlines and linger.

use kafka_client_core::{
    ByteCount, Deadline, ExplicitRecord, PartitionIndex, PayloadId, ProducerBatchPolicy,
    ProducerEffect, ProducerInput, TopicId,
};

use crate::ProducerScenario;

#[test]
fn advancing_past_linger_and_deadline_fires_linger_first() {
    let policy = ProducerBatchPolicy::try_new(10, ByteCount::new(1_024), 10)
        .unwrap_or_else(|error| panic!("valid policy: {error}"));
    let mut scenario = ProducerScenario::with_batch_policy(ByteCount::new(64), 1, policy);
    let payload_id = PayloadId::from_raw(41);
    scenario
        .retain_payload(payload_id, ByteCount::new(32))
        .unwrap_or_else(|error| panic!("payload retention failed: {error}"));
    let admission = scenario
        .step(ProducerInput::AdmitExplicit {
            now: scenario.now(),
            deadline: Deadline::from_tick(15),
            record: ExplicitRecord::new(
                payload_id,
                TopicId::from_raw(7),
                PartitionIndex::from_raw(1),
                ByteCount::new(32),
            ),
        })
        .unwrap_or_else(|error| panic!("admission failed: {error}"));
    let (operation_id, batch_id) = match admission.effects().first() {
        Some(ProducerEffect::AccumulateExplicit {
            operation_id,
            batch_id,
            ..
        }) => (*operation_id, *batch_id),
        effect => panic!("unexpected admission effect: {effect:?}"),
    };
    scenario
        .step(ProducerInput::RecordAccumulated {
            operation_id,
            batch_id,
            accumulator_bytes: ByteCount::new(32),
            now: scenario.now(),
        })
        .unwrap_or_else(|error| panic!("accumulation failed: {error}"));

    scenario
        .advance(20)
        .unwrap_or_else(|error| panic!("virtual advance failed: {error}"));

    assert_eq!(scenario.now().tick(), 20);
    assert!(scenario.contains_batch(batch_id));
    assert!(scenario.contains_payload(payload_id));
    assert!(scenario.terminal_result(operation_id).is_none());
    assert!(scenario.effect_trace().iter().any(|effect| matches!(
        effect,
        ProducerEffect::MaterializeBatch {
            execution,
            ..
        } if execution.batch_id() == batch_id
    )));
}
