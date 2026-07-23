//! Scenarios proving producer effects preserve policy and ownership boundaries.

use kafka_client_core::{
    AdmissionRejection, BatchId, ByteCount, Deadline, DeliveryStatus, ExplicitRecord, OperationId,
    PartitionIndex, PayloadId, ProducerCompletion, ProducerEffect, ProducerInput,
    ProducerMachineError, TopicId,
};

use crate::{ProducerScenario, SimulationError};

const PAYLOAD: PayloadId = PayloadId::from_raw(11);
const BATCH: BatchId = BatchId::from_raw(21);

fn record(payload_id: PayloadId) -> ExplicitRecord {
    ExplicitRecord::new(
        payload_id,
        TopicId::from_raw(31),
        PartitionIndex::from_raw(2),
        ByteCount::new(64),
    )
}

fn admit(scenario: &mut ProducerScenario, payload_id: PayloadId) -> OperationId {
    assert_eq!(
        scenario.retain_payload(payload_id, ByteCount::new(64)),
        Ok(())
    );
    let transition = scenario
        .step(ProducerInput::AdmitExplicit {
            now: scenario.now(),
            deadline: Deadline::from_tick(10),
            record: record(payload_id),
        })
        .unwrap_or_else(|error| panic!("admission failed: {error}"));
    match transition.effects() {
        [ProducerEffect::AccumulateExplicit { operation_id, .. }] => *operation_id,
        effects => panic!("unexpected admission effects: {effects:?}"),
    }
}

fn ready(scenario: &mut ProducerScenario, operation_id: OperationId) {
    assert_eq!(scenario.materialize_batch(BATCH, operation_id), Ok(()));
    scenario
        .step(ProducerInput::BatchReady {
            operation_id,
            batch_id: BATCH,
            now: scenario.now(),
        })
        .unwrap_or_else(|error| panic!("batch readiness failed: {error}"));
}

#[test]
fn success_releases_batch_and_payload_before_publishing_completion() {
    let mut scenario = ProducerScenario::new(ByteCount::new(128), 1);
    let operation_id = admit(&mut scenario, PAYLOAD);
    ready(&mut scenario, operation_id);
    scenario
        .step(ProducerInput::DriverAccepted {
            operation_id,
            batch_id: BATCH,
        })
        .unwrap_or_else(|error| panic!("driver acceptance failed: {error}"));
    scenario
        .step(ProducerInput::BrokerSucceeded {
            operation_id,
            batch_id: BATCH,
        })
        .unwrap_or_else(|error| panic!("broker success failed: {error}"));

    let effects = scenario.effect_trace();
    assert!(matches!(
        effects.get(effects.len() - 3),
        Some(ProducerEffect::ReleaseBatch { batch_id: BATCH })
    ));
    assert!(matches!(
        effects.get(effects.len() - 2),
        Some(ProducerEffect::ReleasePayload {
            payload_id: PAYLOAD,
            ..
        })
    ));
    assert!(matches!(
        effects.last(),
        Some(ProducerEffect::Complete {
            operation_id: completed,
            completion: ProducerCompletion::Delivered,
        }) if *completed == operation_id
    ));
    assert!(!scenario.contains_batch(BATCH));
    assert!(!scenario.contains_payload(PAYLOAD));
    assert_eq!(
        scenario.terminal_result(operation_id),
        Some(ProducerCompletion::Delivered)
    );
}

#[test]
fn expired_materialized_batch_never_reaches_driver_submission() {
    let mut scenario = ProducerScenario::new(ByteCount::new(128), 1);
    let operation_id = admit(&mut scenario, PAYLOAD);
    assert_eq!(scenario.materialize_batch(BATCH, operation_id), Ok(()));
    scenario.advance(10);
    scenario
        .step(ProducerInput::BatchReady {
            operation_id,
            batch_id: BATCH,
            now: scenario.now(),
        })
        .unwrap_or_else(|error| panic!("deadline settlement failed: {error}"));

    assert_eq!(scenario.submission_count(), 0);
    assert_eq!(
        scenario.terminal_result(operation_id),
        Some(ProducerCompletion::Failed(DeliveryStatus::NotSent))
    );
    assert!(!scenario.contains_batch(BATCH));
    assert!(!scenario.contains_payload(PAYLOAD));
}

#[test]
fn driver_rejection_is_terminal_not_sent() {
    let mut scenario = ProducerScenario::new(ByteCount::new(128), 1);
    let operation_id = admit(&mut scenario, PAYLOAD);
    ready(&mut scenario, operation_id);
    scenario
        .step(ProducerInput::DriverRejected {
            operation_id,
            batch_id: BATCH,
        })
        .unwrap_or_else(|error| panic!("driver rejection failed: {error}"));

    assert_eq!(
        scenario.terminal_result(operation_id),
        Some(ProducerCompletion::Failed(DeliveryStatus::NotSent))
    );
}

#[test]
fn driver_possibly_sent_fact_is_preserved_exactly() {
    let mut scenario = ProducerScenario::new(ByteCount::new(128), 1);
    let operation_id = admit(&mut scenario, PAYLOAD);
    ready(&mut scenario, operation_id);
    scenario
        .step(ProducerInput::DriverAccepted {
            operation_id,
            batch_id: BATCH,
        })
        .unwrap_or_else(|error| panic!("driver acceptance failed: {error}"));
    scenario
        .step(ProducerInput::BrokerFailed {
            operation_id,
            batch_id: BATCH,
            delivery: DeliveryStatus::PossiblySent,
        })
        .unwrap_or_else(|error| panic!("broker failure failed: {error}"));

    assert_eq!(
        scenario.terminal_result(operation_id),
        Some(ProducerCompletion::Failed(DeliveryStatus::PossiblySent))
    );
}

#[test]
fn retained_terminal_backpressures_until_engine_result_and_core_marker_reclaim() {
    let mut scenario = ProducerScenario::new(ByteCount::new(128), 1);
    let operation_id = admit(&mut scenario, PAYLOAD);
    ready(&mut scenario, operation_id);
    scenario
        .step(ProducerInput::DriverRejected {
            operation_id,
            batch_id: BATCH,
        })
        .unwrap_or_else(|error| panic!("driver rejection failed: {error}"));

    assert_eq!(scenario.retained_bytes(), ByteCount::new(0));
    assert_eq!(scenario.completion_slots(), 1);
    assert_eq!(
        scenario.retain_payload(PayloadId::from_raw(12), ByteCount::new(64)),
        Ok(())
    );
    assert_eq!(
        scenario.step(ProducerInput::AdmitExplicit {
            now: scenario.now(),
            deadline: Deadline::from_tick(20),
            record: record(PayloadId::from_raw(12)),
        }),
        Err(SimulationError::Core(ProducerMachineError::Admission(
            AdmissionRejection::CompletionCapacity
        )))
    );
    assert!(scenario.contains_payload(PayloadId::from_raw(12)));
    assert_eq!(scenario.retained_bytes(), ByteCount::new(0));
    assert_eq!(
        scenario.step(ProducerInput::CompletionReclaimed { operation_id }),
        Err(SimulationError::TerminalStillRetained(operation_id))
    );
    assert_eq!(scenario.completion_slots(), 1);

    assert_eq!(
        scenario.release_terminal_result(operation_id),
        Ok(ProducerCompletion::Failed(DeliveryStatus::NotSent))
    );
    assert_eq!(scenario.terminal_result(operation_id), None);
    assert_eq!(scenario.completion_slots(), 1);
    scenario
        .step(ProducerInput::CompletionReclaimed { operation_id })
        .unwrap_or_else(|error| panic!("core marker reclaim failed: {error}"));
    assert_eq!(scenario.completion_slots(), 0);
}
