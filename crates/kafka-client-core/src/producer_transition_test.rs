//! Scenarios for batch readiness, timer fencing, and terminal fan-out.

use core::num::NonZeroI16;

use crate::{
    AcknowledgementPolicy, BatchExecutionGeneration, BatchExecutionId, BatchId,
    BatchTimerGeneration, ByteCount, CompressionPolicy, Deadline, DeliveryStatus, ExplicitRecord,
    Moment, OperationId, PartitionIndex, PayloadId, ProducerBatchPolicy, ProducerBatchSuccess,
    ProducerBrokerFailure, ProducerBrokerFailureKind, ProducerCompletion, ProducerEffect,
    ProducerFailureKind, ProducerInput, ProducerMachine, RecordMetadata, TopicId,
};

const TOPIC: TopicId = TopicId::from_raw(9);
const PARTITION: PartitionIndex = PartitionIndex::from_raw(2);

fn execution(batch_id: BatchId) -> BatchExecutionId {
    BatchExecutionId::new(batch_id, BatchExecutionGeneration::initial())
}

fn record(payload: u64) -> ExplicitRecord {
    ExplicitRecord::new(
        PayloadId::from_raw(payload),
        TOPIC,
        PARTITION,
        ByteCount::new(32),
    )
}

fn policy(records: usize, bytes: u64, linger: u64) -> ProducerBatchPolicy {
    ProducerBatchPolicy::try_new(records, ByteCount::new(bytes), linger)
        .unwrap_or_else(|error| panic!("valid test policy: {error}"))
}

fn admit(
    producer: &mut ProducerMachine,
    payload: u64,
    deadline: u64,
) -> (OperationId, BatchId, Vec<ProducerEffect>) {
    let transition = producer
        .apply(ProducerInput::AdmitExplicit {
            now: Moment::from_tick(0),
            deadline: Deadline::from_tick(deadline),
            record: record(payload),
        })
        .unwrap_or_else(|error| panic!("admission failed: {error}"));
    let [
        ProducerEffect::AccumulateExplicit {
            operation_id,
            batch_id,
            ..
        },
        rest @ ..,
    ] = transition.effects()
    else {
        panic!("missing accumulation effect")
    };
    (*operation_id, *batch_id, rest.to_vec())
}

fn accumulated(
    producer: &mut ProducerMachine,
    operation_id: OperationId,
    batch_id: BatchId,
    accumulator_bytes: u64,
) -> Vec<ProducerEffect> {
    producer
        .apply(ProducerInput::RecordAccumulated {
            operation_id,
            batch_id,
            accumulator_bytes: ByteCount::new(accumulator_bytes),
            now: Moment::from_tick(0),
        })
        .unwrap_or_else(|error| panic!("accumulation failed: {error}"))
        .effects()
        .to_vec()
}

fn materialized(producer: &mut ProducerMachine, batch_id: BatchId) -> Vec<ProducerEffect> {
    producer
        .apply(ProducerInput::BatchMaterialized {
            execution: execution(batch_id),
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("materialization failed: {error}"))
        .effects()
        .to_vec()
}

#[test]
fn count_ready_batch_fans_success_out_in_membership_order() {
    let mut producer =
        ProducerMachine::with_batch_policy(ByteCount::new(128), 4, policy(2, 1_024, 50));
    let (first, batch_id, first_tail) = admit(&mut producer, 1, 100);
    assert!(matches!(
        first_tail.as_slice(),
        [ProducerEffect::ArmBatchTimer {
            batch_id: armed,
            generation,
            deadline,
        }] if *armed == batch_id
            && *generation == BatchTimerGeneration::from_raw(1)
            && *deadline == Deadline::from_tick(50)
    ));
    assert!(accumulated(&mut producer, first, batch_id, 40).is_empty());

    let (second, second_batch, second_tail) = admit(&mut producer, 2, 100);
    assert_eq!(second_batch, batch_id);
    assert!(second_tail.is_empty());
    assert_eq!(
        accumulated(&mut producer, second, batch_id, 40),
        vec![
            ProducerEffect::CancelBatchTimer {
                batch_id,
                generation: BatchTimerGeneration::from_raw(1),
            },
            ProducerEffect::MaterializeBatch {
                execution: execution(batch_id),
                compression: CompressionPolicy::Uncompressed,
            },
        ]
    );
    assert!(
        producer
            .apply(ProducerInput::BatchTimerFired {
                batch_id,
                generation: BatchTimerGeneration::from_raw(1),
                now: Moment::from_tick(50),
            })
            .is_ok_and(|transition| transition.effects().is_empty())
    );
    assert_eq!(
        materialized(&mut producer, batch_id),
        vec![ProducerEffect::SubmitProduce {
            execution: execution(batch_id),
            deadline_operation_id: first,
            deadline: Deadline::from_tick(100),
            topic_id: TOPIC,
            partition: PARTITION,
            acknowledgements: AcknowledgementPolicy::All,
        }]
    );
    assert!(
        producer
            .apply(ProducerInput::DriverAccepted {
                execution: execution(batch_id),
            })
            .is_ok()
    );
    let terminal = producer
        .apply(ProducerInput::BrokerSucceeded {
            batch_id,
            success: ProducerBatchSuccess::new(70, Some(11), Some(3)),
        })
        .unwrap_or_else(|error| panic!("success failed: {error}"));

    assert_eq!(terminal.effects().len(), 5);
    assert_eq!(
        terminal.effects()[0],
        ProducerEffect::ReleaseBatch { batch_id }
    );
    assert!(matches!(
        terminal.effects()[1..3],
        [
            ProducerEffect::ReleasePayload { payload_id: first_payload, .. },
            ProducerEffect::ReleasePayload { payload_id: second_payload, .. },
        ] if first_payload.get() == 1 && second_payload.get() == 2
    ));
    assert_eq!(
        terminal.effects()[3..],
        [
            ProducerEffect::Complete {
                operation_id: first,
                completion: ProducerCompletion::Delivered(RecordMetadata::new(
                    PARTITION,
                    70,
                    Some(11),
                    Some(3),
                )),
            },
            ProducerEffect::Complete {
                operation_id: second,
                completion: ProducerCompletion::Delivered(RecordMetadata::new(
                    PARTITION,
                    71,
                    Some(11),
                    Some(3),
                )),
            },
        ]
    );
}

#[test]
fn conservative_accumulator_size_threshold_is_core_owned() {
    let mut producer =
        ProducerMachine::with_batch_policy(ByteCount::new(64), 2, policy(10, 100, 50));
    let (operation_id, batch_id, _) = admit(&mut producer, 1, 100);
    assert_eq!(
        accumulated(&mut producer, operation_id, batch_id, 100),
        vec![
            ProducerEffect::CancelBatchTimer {
                batch_id,
                generation: BatchTimerGeneration::from_raw(1),
            },
            ProducerEffect::MaterializeBatch {
                execution: execution(batch_id),
                compression: CompressionPolicy::Uncompressed,
            },
        ]
    );
}

#[test]
fn broker_failure_preserves_semantic_code_and_certainty() {
    let mut producer = ProducerMachine::new(ByteCount::new(64), 1);
    let (operation_id, batch_id, _) = admit(&mut producer, 1, 100);
    accumulated(&mut producer, operation_id, batch_id, 20);
    producer
        .apply(ProducerInput::BatchMaterialized {
            execution: execution(batch_id),
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("materialization failed: {error}"));
    producer
        .apply(ProducerInput::DriverAccepted {
            execution: execution(batch_id),
        })
        .unwrap_or_else(|error| panic!("driver acceptance failed: {error}"));
    let terminal = producer
        .apply(ProducerInput::BrokerFailed {
            batch_id,
            failure: routing_failure(),
            delivery: DeliveryStatus::PossiblySent,
        })
        .unwrap_or_else(|error| panic!("broker failure failed: {error}"));
    assert!(matches!(
        terminal.effects().last(),
        Some(ProducerEffect::Complete {
            operation_id: completed,
            completion: ProducerCompletion::Failed(actual),
        }) if *completed == operation_id
            && actual.kind() == ProducerFailureKind::Routing
            && actual.broker_code() == Some(6)
            && actual.delivery() == DeliveryStatus::PossiblySent
    ));
}

fn routing_failure() -> ProducerBrokerFailure {
    let code =
        NonZeroI16::new(6).unwrap_or_else(|| panic!("the test broker code must be non-zero"));
    ProducerBrokerFailure::new(ProducerBrokerFailureKind::Routing, code)
}
