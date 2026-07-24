//! Exact sealed-membership execution and durable-history scenarios.

use kafka_client_core::{
    AcknowledgementPolicy, BatchExecutionGeneration, BatchExecutionId, BatchId, ByteCount,
    CompressionPolicy, Deadline, ExplicitRecord, OperationId, PartitionIndex, PayloadId,
    ProducerEffect, ProducerIdentity, ProducerSequenceLease, TopicId,
};

use crate::{SimulationError, state::VirtualProducerState};

fn execution() -> BatchExecutionId {
    BatchExecutionId::new(BatchId::from_raw(1), BatchExecutionGeneration::initial())
}

fn accumulate(operation_id: OperationId, payload_id: PayloadId) -> ProducerEffect {
    ProducerEffect::AccumulateExplicit {
        operation_id,
        batch_id: execution().batch_id(),
        deadline: Deadline::from_tick(20),
        record: ExplicitRecord::new(
            payload_id,
            TopicId::from_raw(2),
            PartitionIndex::from_raw(3),
            ByteCount::new(8),
        ),
    }
}

#[test]
fn simulator_submits_only_exact_materialized_membership_snapshot() {
    let mut state = VirtualProducerState::default();
    let members = [OperationId::from_raw(4), OperationId::from_raw(5)];
    for (operation_id, payload_id) in members
        .into_iter()
        .zip([PayloadId::from_raw(6), PayloadId::from_raw(7)])
    {
        state
            .retain_payload(payload_id, ByteCount::new(8))
            .unwrap_or_else(|error| panic!("payload retention failed: {error}"));
        state
            .interpret(accumulate(operation_id, payload_id))
            .unwrap_or_else(|error| panic!("accumulation failed: {error}"));
    }
    let current = execution();
    state
        .interpret(ProducerEffect::MaterializeBatch {
            execution: current,
            deadline_operation_id: members[0],
            deadline: Deadline::from_tick(20),
            compression: CompressionPolicy::None,
            identity: identity(),
            sequence: sequence(2),
        })
        .unwrap_or_else(|error| panic!("materialization failed: {error}"));
    let stale = BatchExecutionId::new(
        current.batch_id(),
        BatchExecutionGeneration::try_from_raw(2)
            .unwrap_or_else(|| panic!("second generation must be valid")),
    );
    assert_eq!(
        state.interpret(ProducerEffect::SubmitProduce {
            execution: stale,
            deadline_operation_id: members[0],
            deadline: Deadline::from_tick(20),
            topic_id: TopicId::from_raw(2),
            partition: PartitionIndex::from_raw(3),
            acknowledgements: AcknowledgementPolicy::All,
        }),
        Err(SimulationError::BatchExecutionMismatch {
            expected: Some(current),
            actual: stale,
        })
    );
    state
        .interpret(ProducerEffect::SubmitProduce {
            execution: current,
            deadline_operation_id: members[0],
            deadline: Deadline::from_tick(20),
            topic_id: TopicId::from_raw(2),
            partition: PartitionIndex::from_raw(3),
            acknowledgements: AcknowledgementPolicy::All,
        })
        .unwrap_or_else(|error| panic!("submission failed: {error}"));

    assert_eq!(state.submitted_members(current), Some(members.as_slice()));
    assert_eq!(state.submitted_members(stale), None);
    assert_eq!(state.submission_count(), 1);
    state
        .interpret(ProducerEffect::ReleaseBatch {
            batch_id: current.batch_id(),
        })
        .unwrap_or_else(|error| panic!("batch release failed: {error}"));
    assert_eq!(state.submission_count(), 1);
    assert_eq!(state.submitted_members(current), Some(members.as_slice()));
}

#[test]
fn sealed_batch_rejects_delayed_membership_effects() {
    let mut state = VirtualProducerState::default();
    let current = execution();
    let admitted = OperationId::from_raw(8);
    let admitted_payload = PayloadId::from_raw(9);
    state
        .retain_payload(admitted_payload, ByteCount::new(8))
        .unwrap_or_else(|error| panic!("payload retention failed: {error}"));
    state
        .interpret(accumulate(admitted, admitted_payload))
        .unwrap_or_else(|error| panic!("accumulation failed: {error}"));
    state
        .interpret(ProducerEffect::MaterializeBatch {
            execution: current,
            deadline_operation_id: admitted,
            deadline: Deadline::from_tick(20),
            compression: CompressionPolicy::None,
            identity: identity(),
            sequence: sequence(1),
        })
        .unwrap_or_else(|error| panic!("materialization failed: {error}"));

    let delayed = OperationId::from_raw(10);
    let delayed_payload = PayloadId::from_raw(11);
    state
        .retain_payload(delayed_payload, ByteCount::new(8))
        .unwrap_or_else(|error| panic!("delayed payload retention failed: {error}"));
    assert_eq!(
        state.interpret(accumulate(delayed, delayed_payload)),
        Err(SimulationError::BatchMembershipClosed(current.batch_id()))
    );
    assert_eq!(
        state.interpret(ProducerEffect::RemoveBatchMember {
            batch_id: current.batch_id(),
            operation_id: admitted,
        }),
        Err(SimulationError::BatchMembershipClosed(current.batch_id()))
    );
}

#[test]
fn duplicate_submission_history_is_rejected_before_phase_mutation() {
    let mut state = VirtualProducerState::default();
    let current = execution();
    let operation_id = OperationId::from_raw(12);
    let payload_id = PayloadId::from_raw(13);
    state
        .retain_payload(payload_id, ByteCount::new(8))
        .unwrap_or_else(|error| panic!("payload retention failed: {error}"));
    state
        .interpret(accumulate(operation_id, payload_id))
        .unwrap_or_else(|error| panic!("accumulation failed: {error}"));
    state
        .interpret(ProducerEffect::MaterializeBatch {
            execution: current,
            deadline_operation_id: operation_id,
            deadline: Deadline::from_tick(20),
            compression: CompressionPolicy::None,
            identity: identity(),
            sequence: sequence(1),
        })
        .unwrap_or_else(|error| panic!("materialization failed: {error}"));
    state.submissions.insert(current, vec![operation_id]);
    let submit = ProducerEffect::SubmitProduce {
        execution: current,
        deadline_operation_id: operation_id,
        deadline: Deadline::from_tick(20),
        topic_id: TopicId::from_raw(2),
        partition: PartitionIndex::from_raw(3),
        acknowledgements: AcknowledgementPolicy::All,
    };

    assert_eq!(
        state.interpret(submit),
        Err(SimulationError::DuplicateBatchExecution(current))
    );
    state.submissions.remove(&current);
    state
        .interpret(submit)
        .unwrap_or_else(|error| panic!("phase changed before retry: {error}"));
    assert_eq!(
        state.submitted_members(current),
        Some([operation_id].as_slice())
    );
}

fn identity() -> ProducerIdentity {
    ProducerIdentity::try_new(1, 0).unwrap_or_else(|| panic!("test identity must be valid"))
}

fn sequence(record_count: u32) -> ProducerSequenceLease {
    ProducerSequenceLease::try_new(0, record_count)
        .unwrap_or_else(|| panic!("test sequence must be valid"))
}
