//! Producer-transition identity views derived from their ordered effects.

use crate::{
    ByteCount, Deadline, ExplicitRecord, FlushId, Moment, OperationId, PartitionIndex, PayloadId,
    ProducerEffect, ProducerInput, ProducerMachine, ProducerTransition, TopicId,
};

fn record(payload: u64) -> ExplicitRecord {
    ExplicitRecord::new(
        PayloadId::from_raw(payload),
        TopicId::from_raw(9),
        PartitionIndex::from_raw(2),
        ByteCount::new(32),
    )
}

#[test]
fn admission_transition_exposes_accepted_identity_and_moves_ordered_effects() {
    let mut producer = ProducerMachine::new(ByteCount::new(64), 1);
    let transition = admit(&mut producer);
    let operation_id = transition
        .admitted_operation_id()
        .unwrap_or_else(|| panic!("accepted transition must identify its operation"));
    let expected = transition.effects().to_vec();

    assert_eq!(operation_id, OperationId::from_raw(1));
    let mut owned = transition.into_effects();
    assert_eq!(owned, expected);
    owned.rotate_left(1);
    assert_eq!(
        ProducerTransition::from_effects(owned).admitted_operation_id(),
        Some(operation_id)
    );
}

#[test]
fn immediate_flush_transition_exposes_accepted_identity() {
    let mut producer = ProducerMachine::new(ByteCount::new(64), 1);
    let transition = producer
        .apply(ProducerInput::FlushRequested)
        .unwrap_or_else(|error| panic!("immediate flush failed: {error}"));
    let flush_id = FlushId::from_raw(1);

    assert_eq!(transition.accepted_flush_id(), Some(flush_id));
    assert!(matches!(
        transition.effects(),
        [
            ProducerEffect::AcceptFlush {
                flush_id: accepted,
                ..
            },
            ProducerEffect::CompleteFlush {
                flush_id: completed,
            },
        ] if *accepted == flush_id && *completed == flush_id
    ));
}

#[test]
fn pending_flush_transition_exposes_accepted_identity() {
    let mut producer = ProducerMachine::new(ByteCount::new(64), 1);
    let admission = admit(&mut producer);
    assert_eq!(admission.accepted_flush_id(), None);

    let transition = producer
        .apply(ProducerInput::FlushRequested)
        .unwrap_or_else(|error| panic!("pending flush failed: {error}"));
    let flush_id = FlushId::from_raw(1);

    assert_eq!(transition.accepted_flush_id(), Some(flush_id));
    assert!(matches!(
        transition.effects(),
        [ProducerEffect::AcceptFlush {
            flush_id: accepted,
            ..
        }] if *accepted == flush_id
    ));
}

#[test]
fn non_acceptance_transitions_expose_no_accepted_identities() {
    let mut producer = ProducerMachine::new(ByteCount::new(64), 1);
    let transition = admit(&mut producer);
    let [
        ProducerEffect::AccumulateExplicit {
            operation_id,
            batch_id,
            ..
        },
        ..,
    ] = transition.effects()
    else {
        panic!("admission did not request accumulation")
    };
    let (operation_id, batch_id) = (*operation_id, *batch_id);

    assert_eq!(transition.accepted_flush_id(), None);
    let accumulated = producer
        .apply(ProducerInput::RecordAccumulated {
            operation_id,
            batch_id,
            accumulator_bytes: ByteCount::new(32),
            now: Moment::from_tick(0),
        })
        .unwrap_or_else(|error| panic!("accumulation failed: {error}"));
    assert_eq!(accumulated.admitted_operation_id(), None);
    assert_eq!(accumulated.accepted_flush_id(), None);
}

fn admit(producer: &mut ProducerMachine) -> ProducerTransition {
    producer
        .apply(ProducerInput::AdmitExplicit {
            now: Moment::from_tick(0),
            deadline: Deadline::from_tick(100),
            record: record(1),
        })
        .unwrap_or_else(|error| panic!("admission failed: {error}"))
}
