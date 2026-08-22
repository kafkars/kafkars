//! Chronological virtual-time scenarios for deadlines, linger, and identity backoff.

use core::num::NonZeroI16;

use kafka_client_core::{
    ByteCount, Deadline, ExplicitRecord, PartitionIndex, PayloadId, ProducerBatchPolicy,
    ProducerCompletion, ProducerEffect, ProducerFailureKind, ProducerIdentityGeneration,
    ProducerInput, TopicId,
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

#[test]
fn virtual_time_retains_and_fires_producer_identity_backoff() {
    const BACKOFF: u64 = 100_000_000;
    let batch_policy = ProducerBatchPolicy::try_new(1, ByteCount::new(1_024), 10)
        .unwrap_or_else(|error| panic!("batch policy must be valid: {error}"));
    let mut scenario = ProducerScenario::with_batch_policy(ByteCount::new(128), 1, batch_policy);
    scenario.disable_automatic_identity_for_test();
    let payload_id = PayloadId::from_raw(1);
    scenario
        .retain_payload(payload_id, ByteCount::new(8))
        .unwrap_or_else(|error| panic!("payload retention failed: {error}"));
    let admitted = scenario
        .step(ProducerInput::AdmitExplicit {
            now: scenario.now(),
            deadline: Deadline::from_tick(BACKOFF + 50),
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
            now: scenario.now(),
        })
        .unwrap_or_else(|error| panic!("accumulation failed: {error}"));
    scenario
        .step(ProducerInput::ProducerIdentityFailed {
            generation: ProducerIdentityGeneration::initial(),
            broker_code: NonZeroI16::new(14),
            now: scenario.now(),
        })
        .unwrap_or_else(|error| panic!("coordinator load failed: {error}"));

    scenario
        .advance(BACKOFF - 1)
        .unwrap_or_else(|error| panic!("pre-backoff advance failed: {error}"));
    assert_eq!(identity_acquisitions(&scenario), 1);
    scenario
        .advance(1)
        .unwrap_or_else(|error| panic!("identity retry advance failed: {error}"));
    assert_eq!(identity_acquisitions(&scenario), 2);
    assert!(scenario.effect_trace().iter().any(|effect| matches!(
        effect,
        ProducerEffect::AcquireProducerIdentity { generation, .. }
            if generation.get() == 2
    )));
}

#[test]
fn identity_retry_wins_an_equal_batch_deadline_in_virtual_time() {
    let policy = ProducerBatchPolicy::try_new(1, ByteCount::new(1_024), 10)
        .unwrap_or_else(|error| panic!("batch policy must be valid: {error}"));
    let mut scenario = ProducerScenario::with_batch_policy(ByteCount::new(128), 2, policy);
    scenario.disable_automatic_identity_for_test();
    let expired = admit_identity_waiter(&mut scenario, 1, 2, 50);
    let later = admit_identity_waiter(&mut scenario, 2, 3, 200);
    scenario
        .step(ProducerInput::ProducerIdentityFailed {
            generation: ProducerIdentityGeneration::initial(),
            broker_code: NonZeroI16::new(14),
            now: scenario.now(),
        })
        .unwrap_or_else(|error| panic!("coordinator load failed: {error}"));

    scenario
        .advance(50)
        .unwrap_or_else(|error| panic!("equal deadline advance failed: {error}"));
    for (operation_id, expected) in [
        (expired, ProducerFailureKind::DeadlineElapsed),
        (later, ProducerFailureKind::ProducerIdentity),
    ] {
        let Some(ProducerCompletion::Failed(failure)) = scenario.terminal_result(operation_id)
        else {
            panic!("identity waiter must have one terminal failure")
        };
        assert_eq!(failure.kind(), expected);
    }
    assert_eq!(identity_acquisitions(&scenario), 1);
}

fn admit_identity_waiter(
    scenario: &mut ProducerScenario,
    payload: u64,
    topic: u64,
    deadline: u64,
) -> kafka_client_core::OperationId {
    let payload_id = PayloadId::from_raw(payload);
    scenario
        .retain_payload(payload_id, ByteCount::new(8))
        .unwrap_or_else(|error| panic!("payload retention failed: {error}"));
    let admitted = scenario
        .step(ProducerInput::AdmitExplicit {
            now: scenario.now(),
            deadline: Deadline::from_tick(deadline),
            record: ExplicitRecord::new(
                payload_id,
                TopicId::from_raw(topic),
                PartitionIndex::from_raw(0),
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
            now: scenario.now(),
        })
        .unwrap_or_else(|error| panic!("accumulation failed: {error}"));
    operation_id
}

fn identity_acquisitions(scenario: &ProducerScenario) -> usize {
    scenario
        .effect_trace()
        .iter()
        .filter(|effect| matches!(effect, ProducerEffect::AcquireProducerIdentity { .. }))
        .count()
}
