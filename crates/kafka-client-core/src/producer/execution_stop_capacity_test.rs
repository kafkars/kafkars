//! Combined record and flush transition-capacity scenarios.

use crate::{
    ByteCount, EXECUTION_STOP_EFFECTS_PER_FLUSH, EXECUTION_STOP_EFFECTS_PER_RECORD,
    ProducerBatchPolicy, ProducerEffect, ProducerInput, ProducerMachine, ProducerMachineError,
    TransitionError, execution_stop_effect_capacity, producer_transition_effect_capacity,
};

use super::execution_stop_test::{admit, producer};

#[test]
fn execution_loss_uses_the_combined_record_and_flush_bound() {
    const CAPACITY: usize = 4;
    let mut producer = producer(CAPACITY);
    for payload in 1..=CAPACITY {
        let partition =
            u32::try_from(payload).unwrap_or_else(|error| panic!("test partition: {error}"));
        let _admitted = admit(&mut producer, payload as u64, partition);
        let accepted_flush = producer
            .apply(ProducerInput::FlushRequested)
            .unwrap_or_else(|error| panic!("flush request failed: {error}"));
        assert!(matches!(
            accepted_flush.effects(),
            [ProducerEffect::AcceptFlush { .. }]
        ));
    }

    let terminal = producer
        .apply(ProducerInput::ExecutionUnavailable)
        .unwrap_or_else(|error| panic!("execution settlement failed: {error}"));
    let expected = execution_stop_effect_capacity(CAPACITY, CAPACITY)
        .unwrap_or_else(|| panic!("small test capacity must be representable"));

    assert_eq!(
        producer.transition_effect_capacity(),
        producer_transition_effect_capacity(CAPACITY, CAPACITY)
    );
    assert_eq!(producer.transition_effect_capacity(), Some(expected));
    assert_eq!(terminal.effects().len(), expected);
    assert!(
        terminal.effects()[CAPACITY * EXECUTION_STOP_EFFECTS_PER_RECORD..]
            .iter()
            .all(|effect| matches!(effect, ProducerEffect::CompleteFlush { .. }))
    );
    assert_eq!(producer.completion_slots(), CAPACITY);
    assert_eq!(producer.flush_slots(), CAPACITY);
    assert_eq!(producer.retained_bytes(), ByteCount::new(0));
    assert_eq!(EXECUTION_STOP_EFFECTS_PER_FLUSH, 1);
}

#[test]
fn combined_transition_capacity_reports_arithmetic_overflow() {
    assert_eq!(execution_stop_effect_capacity(usize::MAX, 1), None);
    assert_eq!(execution_stop_effect_capacity(1, usize::MAX), None);
    assert_eq!(producer_transition_effect_capacity(usize::MAX, 1), None);
    assert_eq!(producer_transition_effect_capacity(1, usize::MAX), None);
}

#[test]
fn empty_producer_capacity_covers_immediate_flush_acceptance_and_completion() {
    let mut producer = ProducerMachine::with_batch_policy_and_flush_capacity(
        ByteCount::new(0),
        0,
        ProducerBatchPolicy::single_record(),
        1,
    );

    assert_eq!(producer.transition_effect_capacity(), Some(2));
    let transition = producer
        .apply(ProducerInput::FlushRequested)
        .unwrap_or_else(|error| panic!("empty flush should settle immediately: {error}"));
    assert!(matches!(
        transition.effects(),
        [
            ProducerEffect::AcceptFlush { .. },
            ProducerEffect::CompleteFlush { .. }
        ]
    ));
}

#[test]
fn unrepresentable_transition_capacity_rejects_before_dispatch() {
    let mut producer = ProducerMachine::new(ByteCount::new(0), usize::MAX);

    assert_eq!(
        producer.apply(ProducerInput::ExecutionUnavailable),
        Err(ProducerMachineError::Transition(
            TransitionError::InvalidState
        ))
    );
    assert!(producer.admission_is_open());
    assert_eq!(producer.completion_slots(), 0);
    assert_eq!(producer.flush_slots(), 0);
    assert_eq!(producer.retained_bytes(), ByteCount::new(0));
}

#[test]
fn execution_stop_preflight_failure_leaves_lifecycle_and_capacity_unchanged() {
    let mut producer = producer(1);
    let (operation_id, _batch_id) = admit(&mut producer, 1, 0);
    let before_state = producer
        .operation(operation_id)
        .map(crate::ProducerOperation::state);
    let _missing_record = producer.records.remove(&operation_id);

    assert_eq!(
        producer.apply(ProducerInput::ExecutionUnavailable),
        Err(ProducerMachineError::UnknownOperation)
    );
    assert_eq!(
        producer
            .operation(operation_id)
            .map(crate::ProducerOperation::state),
        before_state
    );
    assert!(producer.admission_is_open());
    assert_eq!(producer.completion_slots(), 1);
    assert_eq!(producer.retained_bytes(), ByteCount::new(11));
}
