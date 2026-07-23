//! Exact-generation outcome facts distinguish stale work from lifecycle corruption.

use crate::{
    BatchExecutionGeneration, BatchExecutionId, BatchId, ByteCount, Deadline, ExplicitRecord,
    Moment, PartitionIndex, PayloadId, ProducerEffect, ProducerInput, ProducerMachine,
    ProducerMachineError, TopicId, TransitionError,
};

fn materializing() -> (ProducerMachine, BatchExecutionId) {
    let mut producer = ProducerMachine::new(ByteCount::new(64), 1);
    let admitted = producer
        .apply(ProducerInput::AdmitExplicit {
            now: Moment::from_tick(0),
            deadline: Deadline::from_tick(20),
            record: ExplicitRecord::new(
                PayloadId::from_raw(1),
                TopicId::from_raw(2),
                PartitionIndex::from_raw(3),
                ByteCount::new(8),
            ),
        })
        .unwrap_or_else(|error| panic!("admission failed: {error}"));
    let Some(ProducerEffect::AccumulateExplicit {
        operation_id,
        batch_id,
        ..
    }) = admitted.effects().first()
    else {
        panic!("admission did not name batch membership")
    };
    let sealed = producer
        .apply(ProducerInput::RecordAccumulated {
            operation_id: *operation_id,
            batch_id: *batch_id,
            accumulator_bytes: ByteCount::new(8),
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("accumulation failed: {error}"));
    let execution = sealed
        .effects()
        .iter()
        .find_map(|effect| match effect {
            ProducerEffect::MaterializeBatch { execution, .. } => Some(*execution),
            _ => None,
        })
        .unwrap_or_else(|| panic!("seal did not name an execution"));
    (producer, execution)
}

fn next(execution: BatchExecutionId) -> BatchExecutionId {
    let generation = BatchExecutionGeneration::try_from_raw(2)
        .unwrap_or_else(|| panic!("second generation must be valid"));
    BatchExecutionId::new(execution.batch_id(), generation)
}

fn unknown(execution: BatchExecutionId) -> BatchExecutionId {
    BatchExecutionId::new(BatchId::from_raw(999), execution.generation())
}

fn invalid(result: &Result<crate::ProducerTransition, ProducerMachineError>) {
    assert_eq!(
        result,
        &Err(ProducerMachineError::Transition(
            TransitionError::InvalidState
        ))
    );
}

#[test]
fn generation_fenced_inputs_distinguish_stale_facts_from_corruption() {
    let (mut producer, current) = materializing();
    let stale = next(current);
    for input in [
        ProducerInput::BatchMaterialized {
            execution: stale,
            now: Moment::from_tick(2),
        },
        ProducerInput::BatchMaterializationFailed { execution: stale },
        ProducerInput::DriverRejected { execution: stale },
        ProducerInput::BatchMaterialized {
            execution: unknown(current),
            now: Moment::from_tick(2),
        },
        ProducerInput::BatchMaterializationFailed {
            execution: unknown(current),
        },
        ProducerInput::DriverRejected {
            execution: unknown(current),
        },
    ] {
        assert!(
            producer
                .apply(input)
                .is_ok_and(|transition| transition.effects().is_empty())
        );
    }
    for reported in [stale, unknown(current)] {
        assert_eq!(
            producer.apply(ProducerInput::DriverAccepted {
                execution: reported,
            }),
            Err(ProducerMachineError::StaleDriverAcceptance {
                reported,
                current: (reported.batch_id() == current.batch_id()).then_some(current),
            })
        );
    }
    assert!(
        producer
            .apply(ProducerInput::BatchMaterialized {
                execution: current,
                now: Moment::from_tick(2),
            })
            .is_ok()
    );
}

#[test]
fn exact_materialized_fact_in_wrong_phase_is_invalid() {
    let (mut producer, execution) = materializing();
    producer
        .apply(ProducerInput::BatchMaterialized {
            execution,
            now: Moment::from_tick(2),
        })
        .unwrap_or_else(|error| panic!("first materialized fact failed: {error}"));
    invalid(&producer.apply(ProducerInput::BatchMaterialized {
        execution,
        now: Moment::from_tick(3),
    }));
}

#[test]
fn exact_materialization_failure_in_wrong_phase_is_invalid() {
    let (mut producer, execution) = materializing();
    producer
        .apply(ProducerInput::BatchMaterialized {
            execution,
            now: Moment::from_tick(2),
        })
        .unwrap_or_else(|error| panic!("materialized fact failed: {error}"));
    invalid(&producer.apply(ProducerInput::BatchMaterializationFailed { execution }));
}

#[test]
fn exact_driver_acceptance_in_wrong_phase_is_invalid() {
    let (mut producer, execution) = materializing();
    invalid(&producer.apply(ProducerInput::DriverAccepted { execution }));
}

#[test]
fn exact_driver_rejection_in_wrong_phase_is_invalid() {
    let (mut producer, execution) = materializing();
    invalid(&producer.apply(ProducerInput::DriverRejected { execution }));
}

#[test]
fn sealed_generation_is_nonzero_initial_identity() {
    assert_eq!(BatchExecutionGeneration::try_from_raw(0), None);
    let (_producer, execution) = materializing();
    assert_eq!(execution.generation(), BatchExecutionGeneration::initial());
    assert_eq!(execution.generation().get(), 1);
}
