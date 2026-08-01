//! Producer metadata and certainty translation scenarios at the engine boundary.

use kafka_client_core::{
    BatchExecutionGeneration, BatchExecutionId, BatchId, ByteCount, Deadline, DeliveryStatus,
    ExplicitRecord, Moment, PartitionIndex, PayloadId, ProducerAttemptFailureKind,
    ProducerBatchSuccess, ProducerCompletion, ProducerEffect, ProducerIdentityGeneration,
    ProducerInput, ProducerMachine, ProducerTransition, TopicId,
};

use crate::{
    ProducerDeliveryError, ProducerDeliveryFailureKind, ProducerDeliveryObserver,
    ProducerDeliveryResult, ProducerDeliveryStatus, completion::CompletionRegistry,
    producer::ProducerTerminal,
};

fn execution(batch_id: BatchId) -> BatchExecutionId {
    BatchExecutionId::new(batch_id, BatchExecutionGeneration::initial())
}

#[test]
fn delivered_metadata_translates_without_core_types() {
    let result = observe(delivered_completion());
    let metadata = match result {
        Ok(metadata) => metadata,
        Err(error) => panic!("delivery should succeed: {error}"),
    };

    assert_eq!(metadata.partition(), 3);
    assert_eq!(metadata.offset(), 42);
    assert_eq!(metadata.append_timestamp(), Some(900));
    assert_eq!(metadata.leader_epoch(), Some(7));
}

#[test]
fn failure_certainty_preserves_not_sent_and_possibly_sent() {
    let Err(ProducerDeliveryError::Failed(not_sent)) = observe(not_sent_completion()) else {
        panic!("materialization failure should be terminal")
    };
    assert_eq!(
        not_sent.kind(),
        ProducerDeliveryFailureKind::MaterializationFailed
    );
    assert_eq!(not_sent.delivery_status(), ProducerDeliveryStatus::NotSent);

    let Err(ProducerDeliveryError::Failed(possibly_sent)) = observe(possibly_sent_completion())
    else {
        panic!("transport failure should be terminal")
    };
    assert_eq!(possibly_sent.kind(), ProducerDeliveryFailureKind::Transport);
    assert_eq!(
        possibly_sent.delivery_status(),
        ProducerDeliveryStatus::PossiblySent
    );

    let Err(ProducerDeliveryError::Failed(invalid_response)) =
        observe(invalid_response_completion())
    else {
        panic!("invalid response should be terminal")
    };
    assert_eq!(
        invalid_response.kind(),
        ProducerDeliveryFailureKind::InvalidResponse
    );
    assert_eq!(
        invalid_response.delivery_status(),
        ProducerDeliveryStatus::PossiblySent
    );

    let Err(ProducerDeliveryError::Failed(stopped)) = observe(execution_unavailable_completion())
    else {
        panic!("execution loss should be terminal")
    };
    assert_eq!(
        stopped.kind(),
        ProducerDeliveryFailureKind::ExecutionUnavailable
    );
    assert_eq!(stopped.delivery_status(), ProducerDeliveryStatus::NotSent);
}

#[test]
fn invalid_response_failure_crosses_engine_boundary_without_becoming_transport() {
    let Err(ProducerDeliveryError::Failed(failure)) = observe(invalid_response_completion()) else {
        panic!("invalid response should be terminal")
    };
    assert_eq!(failure.kind(), ProducerDeliveryFailureKind::InvalidResponse);
    assert_eq!(
        failure.delivery_status(),
        ProducerDeliveryStatus::PossiblySent
    );
    assert_eq!(failure.broker_code(), None);
}

pub(super) fn delivered_completion() -> ProducerCompletion {
    let (mut machine, batch_id) = materializing_machine();
    apply(
        &mut machine,
        ProducerInput::BatchMaterialized {
            execution: execution(batch_id),
            now: Moment::from_tick(2),
        },
    );
    apply(
        &mut machine,
        ProducerInput::DriverAccepted {
            execution: execution(batch_id),
        },
    );
    terminal(
        &mut machine,
        ProducerInput::BrokerSucceeded {
            execution: execution(batch_id),
            success: ProducerBatchSuccess::new(42, Some(900), Some(7)),
        },
    )
}

fn not_sent_completion() -> ProducerCompletion {
    let (mut machine, batch_id) = materializing_machine();
    terminal(
        &mut machine,
        ProducerInput::BatchMaterializationFailed {
            execution: execution(batch_id),
        },
    )
}

fn possibly_sent_completion() -> ProducerCompletion {
    let (mut machine, batch_id) = materializing_machine();
    apply(
        &mut machine,
        ProducerInput::BatchMaterialized {
            execution: execution(batch_id),
            now: Moment::from_tick(2),
        },
    );
    apply(
        &mut machine,
        ProducerInput::DriverAccepted {
            execution: execution(batch_id),
        },
    );
    terminal(
        &mut machine,
        ProducerInput::TransportFailed {
            execution: execution(batch_id),
            now: Moment::from_tick(3),
            failure: ProducerAttemptFailureKind::Permanent,
            delivery: DeliveryStatus::PossiblySent,
            route_refreshed: false,
        },
    )
}

fn invalid_response_completion() -> ProducerCompletion {
    let (mut machine, batch_id) = materializing_machine();
    apply(
        &mut machine,
        ProducerInput::BatchMaterialized {
            execution: execution(batch_id),
            now: Moment::from_tick(2),
        },
    );
    apply(
        &mut machine,
        ProducerInput::DriverAccepted {
            execution: execution(batch_id),
        },
    );
    terminal(
        &mut machine,
        ProducerInput::TransportFailed {
            execution: execution(batch_id),
            now: Moment::from_tick(3),
            failure: ProducerAttemptFailureKind::InvalidResponse,
            delivery: DeliveryStatus::PossiblySent,
            route_refreshed: false,
        },
    )
}

fn execution_unavailable_completion() -> ProducerCompletion {
    let (mut machine, _batch_id) = materializing_machine();
    terminal(&mut machine, ProducerInput::ExecutionUnavailable)
}

fn materializing_machine() -> (ProducerMachine, kafka_client_core::BatchId) {
    let mut machine = ProducerMachine::new(ByteCount::new(64), 1);
    let admitted = apply(
        &mut machine,
        ProducerInput::AdmitExplicit {
            now: Moment::from_tick(1),
            deadline: Deadline::from_tick(50),
            record: ExplicitRecord::new(
                PayloadId::from_raw(1),
                TopicId::from_raw(2),
                PartitionIndex::from_raw(3),
                ByteCount::new(8),
            ),
        },
    );
    let Some((operation_id, batch_id)) =
        admitted.effects().iter().find_map(|effect| match effect {
            ProducerEffect::AccumulateExplicit {
                operation_id,
                batch_id,
                ..
            } => Some((*operation_id, *batch_id)),
            _ => None,
        })
    else {
        panic!("admission should identify its batch")
    };
    let waiting = apply(
        &mut machine,
        ProducerInput::RecordAccumulated {
            operation_id,
            batch_id,
            accumulator_bytes: ByteCount::new(8),
            now: Moment::from_tick(2),
        },
    );
    let generation = identity_generation(&waiting);
    apply(
        &mut machine,
        ProducerInput::ProducerIdentityAcquired {
            generation,
            producer_id: 1,
            producer_epoch: 0,
            now: Moment::from_tick(2),
        },
    );
    (machine, batch_id)
}

fn identity_generation(transition: &ProducerTransition) -> ProducerIdentityGeneration {
    let Some(generation) = transition.effects().iter().find_map(|effect| match effect {
        ProducerEffect::AcquireProducerIdentity { generation, .. } => Some(*generation),
        _ => None,
    }) else {
        panic!("sealed batch should acquire one producer identity")
    };
    generation
}

fn terminal(machine: &mut ProducerMachine, input: ProducerInput) -> ProducerCompletion {
    let transition = apply(machine, input);
    let Some(completion) = transition.effects().iter().find_map(|effect| match effect {
        ProducerEffect::Complete { completion, .. } => Some(*completion),
        _ => None,
    }) else {
        panic!("core transition should be terminal")
    };
    completion
}

fn apply(machine: &mut ProducerMachine, input: ProducerInput) -> ProducerTransition {
    match machine.apply(input) {
        Ok(transition) => transition,
        Err(error) => panic!("core transition should succeed: {error}"),
    }
}

fn observe(completion: ProducerCompletion) -> ProducerDeliveryResult {
    let Ok(mut registry) = CompletionRegistry::new(1, 1) else {
        panic!("completion notifier should start")
    };
    let Ok((completion_id, observer)) = registry.reserve() else {
        panic!("completion slot should reserve")
    };
    let observer = ProducerDeliveryObserver::from_completion(observer);
    assert_eq!(
        registry.publish(completion_id, ProducerTerminal::record(completion)),
        Ok(())
    );
    let result = observer.wait();
    let Ok(join) = registry.stop_notifier() else {
        panic!("settled notifier should stop")
    };
    assert_eq!(join.join_off_notifier(), Ok(()));
    result
}
