//! Scenarios proving virtual execution preserves core ownership decisions.

use core::num::NonZeroI16;

use kafka_client_core::{
    AdmissionRejection, BatchExecutionGeneration, BatchExecutionId, BatchId, ByteCount, Deadline,
    DeliveryStatus, ExplicitRecord, OperationId, PartitionIndex, PayloadId,
    ProducerAttemptFailureKind, ProducerBatchSuccess, ProducerBrokerFailure,
    ProducerBrokerFailureKind, ProducerCompletion, ProducerEffect, ProducerFailureKind,
    ProducerInput, ProducerMachineError, TopicId,
};

use crate::{ProducerScenario, SimulationError};

const PAYLOAD: PayloadId = PayloadId::from_raw(11);
const BATCH: BatchId = BatchId::from_raw(1);

fn execution() -> BatchExecutionId {
    BatchExecutionId::new(BATCH, BatchExecutionGeneration::initial())
}

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
    match transition.effects().first() {
        Some(ProducerEffect::AccumulateExplicit { operation_id, .. }) => *operation_id,
        effects => panic!("unexpected admission effect: {effects:?}"),
    }
}

fn materialized(scenario: &mut ProducerScenario, operation_id: OperationId) {
    scenario
        .step(ProducerInput::RecordAccumulated {
            operation_id,
            batch_id: BATCH,
            accumulator_bytes: ByteCount::new(64),
            now: scenario.now(),
        })
        .unwrap_or_else(|error| panic!("accumulation failed: {error}"));
    scenario
        .step(ProducerInput::BatchMaterialized {
            execution: execution(),
            now: scenario.now(),
        })
        .unwrap_or_else(|error| panic!("materialization failed: {error}"));
}

#[test]
fn success_releases_batch_and_payload_before_completion() {
    let mut scenario = ProducerScenario::new(ByteCount::new(128), 1);
    let operation_id = admit(&mut scenario, PAYLOAD);
    materialized(&mut scenario, operation_id);
    assert!(scenario.effect_trace().iter().any(|effect| matches!(
        effect,
        ProducerEffect::SubmitProduce {
            deadline_operation_id,
            deadline,
            ..
        } if *deadline_operation_id == operation_id && *deadline == Deadline::from_tick(10)
    )));
    scenario
        .step(ProducerInput::DriverAccepted {
            execution: execution(),
        })
        .unwrap_or_else(|error| panic!("driver acceptance failed: {error}"));
    scenario
        .step(ProducerInput::BrokerSucceeded {
            execution: execution(),
            success: ProducerBatchSuccess::new(40, Some(7), Some(3)),
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
            completion: ProducerCompletion::Delivered(metadata),
        }) if *completed == operation_id && metadata.offset() == 40
    ));
    assert!(!scenario.contains_batch(BATCH));
    assert!(!scenario.contains_payload(PAYLOAD));
}

#[test]
fn expired_accumulation_never_reaches_driver_submission() {
    let mut scenario = ProducerScenario::new(ByteCount::new(128), 1);
    let operation_id = admit(&mut scenario, PAYLOAD);
    scenario
        .advance(10)
        .unwrap_or_else(|error| panic!("deadline timer failed: {error}"));

    assert_eq!(scenario.submission_count(), 0);
    assert!(matches!(
        scenario.terminal_result(operation_id),
        Some(ProducerCompletion::Failed(failure))
            if failure.kind() == ProducerFailureKind::DeadlineElapsed
                && failure.delivery() == DeliveryStatus::NotSent
    ));
    assert!(!scenario.contains_batch(BATCH));
    assert!(!scenario.contains_payload(PAYLOAD));
}

#[test]
fn partial_expiry_removes_the_record_before_completion_and_materialization() {
    let policy = kafka_client_core::ProducerBatchPolicy::try_new(10, ByteCount::new(1_024), 20)
        .unwrap_or_else(|error| panic!("valid policy: {error}"));
    let mut scenario = ProducerScenario::with_batch_policy(ByteCount::new(128), 2, policy);
    let first_payload = PayloadId::from_raw(21);
    let second_payload = PayloadId::from_raw(22);
    for payload in [first_payload, second_payload] {
        scenario
            .retain_payload(payload, ByteCount::new(64))
            .unwrap_or_else(|error| panic!("payload retention failed: {error}"));
    }
    let first = scenario
        .step(ProducerInput::AdmitExplicit {
            now: scenario.now(),
            deadline: Deadline::from_tick(100),
            record: record(first_payload),
        })
        .unwrap_or_else(|error| panic!("first admission failed: {error}"));
    let (first_id, batch_id) = match first.effects().first() {
        Some(ProducerEffect::AccumulateExplicit {
            operation_id,
            batch_id,
            ..
        }) => (*operation_id, *batch_id),
        effect => panic!("unexpected first effect: {effect:?}"),
    };
    scenario
        .step(ProducerInput::RecordAccumulated {
            operation_id: first_id,
            batch_id,
            accumulator_bytes: ByteCount::new(64),
            now: scenario.now(),
        })
        .unwrap_or_else(|error| panic!("first accumulation failed: {error}"));
    let second = scenario
        .step(ProducerInput::AdmitExplicit {
            now: scenario.now(),
            deadline: Deadline::from_tick(10),
            record: record(second_payload),
        })
        .unwrap_or_else(|error| panic!("second admission failed: {error}"));
    let second_id = match second.effects().first() {
        Some(ProducerEffect::AccumulateExplicit { operation_id, .. }) => *operation_id,
        effect => panic!("unexpected second effect: {effect:?}"),
    };
    scenario
        .step(ProducerInput::RecordAccumulated {
            operation_id: second_id,
            batch_id,
            accumulator_bytes: ByteCount::new(64),
            now: scenario.now(),
        })
        .unwrap_or_else(|error| panic!("second accumulation failed: {error}"));

    scenario
        .advance(10)
        .unwrap_or_else(|error| panic!("partial expiry failed: {error}"));
    assert!(scenario.contains_batch(batch_id));
    assert!(scenario.contains_payload(first_payload));
    assert!(!scenario.contains_payload(second_payload));
    assert!(matches!(
        scenario.terminal_result(second_id),
        Some(ProducerCompletion::Failed(failure))
            if failure.kind() == ProducerFailureKind::DeadlineElapsed
    ));
    assert!(scenario.effect_trace().iter().any(|effect| matches!(
        effect,
        ProducerEffect::RemoveBatchMember {
            batch_id: removed_batch,
            operation_id,
        } if *removed_batch == batch_id && *operation_id == second_id
    )));
    assert_eq!(scenario.submission_count(), 0);
}

#[test]
fn driver_and_broker_failure_stages_preserve_certainty() {
    let mut rejected = ProducerScenario::new(ByteCount::new(128), 1);
    let rejected_id = admit(&mut rejected, PAYLOAD);
    materialized(&mut rejected, rejected_id);
    rejected
        .step(ProducerInput::DriverRejected {
            execution: execution(),
            now: rejected.now(),
            failure: ProducerAttemptFailureKind::Permanent,
        })
        .unwrap_or_else(|error| panic!("driver rejection failed: {error}"));
    assert!(matches!(
        rejected.terminal_result(rejected_id),
        Some(ProducerCompletion::Failed(failure))
            if failure.delivery() == DeliveryStatus::NotSent
    ));

    let mut uncertain = ProducerScenario::new(ByteCount::new(128), 1);
    let uncertain_id = admit(&mut uncertain, PAYLOAD);
    materialized(&mut uncertain, uncertain_id);
    uncertain
        .step(ProducerInput::DriverAccepted {
            execution: execution(),
        })
        .unwrap_or_else(|error| panic!("driver acceptance failed: {error}"));
    uncertain
        .step(ProducerInput::BrokerFailed {
            execution: execution(),
            failure: routing_failure(),
            delivery: DeliveryStatus::PossiblySent,
        })
        .unwrap_or_else(|error| panic!("broker failure failed: {error}"));
    assert!(matches!(
        uncertain.terminal_result(uncertain_id),
        Some(ProducerCompletion::Failed(failure))
            if failure.delivery() == DeliveryStatus::PossiblySent
    ));
}

fn routing_failure() -> ProducerBrokerFailure {
    let code =
        NonZeroI16::new(6).unwrap_or_else(|| panic!("the test broker code must be non-zero"));
    ProducerBrokerFailure::new(ProducerBrokerFailureKind::Routing, code)
}

#[test]
fn retained_terminal_backpressures_until_result_and_marker_reclaim() {
    let mut scenario = ProducerScenario::new(ByteCount::new(128), 1);
    let operation_id = admit(&mut scenario, PAYLOAD);
    materialized(&mut scenario, operation_id);
    scenario
        .step(ProducerInput::DriverRejected {
            execution: execution(),
            now: scenario.now(),
            failure: ProducerAttemptFailureKind::Permanent,
        })
        .unwrap_or_else(|error| panic!("driver rejection failed: {error}"));

    let second = PayloadId::from_raw(12);
    assert_eq!(scenario.retain_payload(second, ByteCount::new(64)), Ok(()));
    assert_eq!(
        scenario.step(ProducerInput::AdmitExplicit {
            now: scenario.now(),
            deadline: Deadline::from_tick(20),
            record: record(second),
        }),
        Err(SimulationError::Core(ProducerMachineError::Admission(
            AdmissionRejection::CompletionCapacity
        )))
    );
    assert_eq!(
        scenario.step(ProducerInput::CompletionReclaimed { operation_id }),
        Err(SimulationError::TerminalStillRetained(operation_id))
    );
    let result = scenario
        .release_terminal_result(operation_id)
        .unwrap_or_else(|error| panic!("result release failed: {error}"));
    assert!(matches!(result, ProducerCompletion::Failed(_)));
    scenario
        .step(ProducerInput::CompletionReclaimed { operation_id })
        .unwrap_or_else(|error| panic!("core marker reclaim failed: {error}"));
    assert_eq!(scenario.completion_slots(), 0);
}
