//! Behavioral evidence for the fixed-capacity assigned-consumer close slot.

use kafka_client_core::{
    AssignedConsumerCloseId, AssignedConsumerEffect, AssignedConsumerInput,
    AssignedConsumerMachine, AssignedPartition, AssignedTopicPartition, Deadline, Moment,
    NextFetchOffset, PartitionIndex, StartPosition, TopicId,
};

use super::assigned_close_error::{
    AssignedCloseEffectKind, AssignedCloseSlotError, AssignedCloseSlotPhase,
};
use super::assigned_close_slot::AssignedCloseSlot;

#[test]
fn reserved_close_reaches_one_retained_terminal_then_reclaims() {
    let (close_id, mut machine) = accepted_close();
    let mut slot = AssignedCloseSlot::create_for_assigned_owner();

    slot.reserve().unwrap_or_else(|error| panic!("{error:?}"));
    slot.observe_close_effect(AssignedConsumerEffect::AcceptClose { close_id })
        .unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(slot.phase(), AssignedCloseSlotPhase::Accepted);
    assert_eq!(slot.accepted_id(), Ok(close_id));

    let complete = machine
        .apply(AssignedConsumerInput::CloseDrained { close_id })
        .unwrap_or_else(|error| panic!("{error}"));
    slot.observe_close_effect(complete.effects()[0])
        .unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(slot.phase(), AssignedCloseSlotPhase::Ready);
    assert_eq!(slot.take_ready(), Ok(close_id));
    assert_eq!(slot.phase(), AssignedCloseSlotPhase::Reclaimed);
}

#[test]
fn rejected_admission_releases_the_exact_reservation() {
    let (_, mut closed_machine) = accepted_close();
    let mut slot = AssignedCloseSlot::create_for_assigned_owner();

    assert_eq!(
        slot.release_rejected(),
        Err(AssignedCloseSlotError::InvalidRelease {
            phase: AssignedCloseSlotPhase::Vacant,
        })
    );
    slot.reserve().unwrap_or_else(|error| panic!("{error:?}"));
    assert!(
        closed_machine
            .apply(AssignedConsumerInput::BeginClose)
            .is_err()
    );
    slot.release_rejected()
        .unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(slot.phase(), AssignedCloseSlotPhase::Vacant);
    slot.reserve().unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(slot.phase(), AssignedCloseSlotPhase::Reserved);
}

#[test]
fn reservation_and_take_failures_retain_the_exact_phase() {
    let (close_id, _) = accepted_close();
    let mut slot = AssignedCloseSlot::create_for_assigned_owner();

    assert_eq!(
        slot.take_ready(),
        Err(AssignedCloseSlotError::TerminalUnavailable {
            phase: AssignedCloseSlotPhase::Vacant,
        })
    );
    slot.reserve().unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(
        slot.reserve(),
        Err(AssignedCloseSlotError::InvalidReservation {
            phase: AssignedCloseSlotPhase::Reserved,
        })
    );
    slot.observe_close_effect(AssignedConsumerEffect::AcceptClose { close_id })
        .unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(
        slot.release_rejected(),
        Err(AssignedCloseSlotError::InvalidRelease {
            phase: AssignedCloseSlotPhase::Accepted,
        })
    );
    assert_eq!(slot.phase(), AssignedCloseSlotPhase::Accepted);
}

#[test]
fn accepted_identity_query_is_narrow_and_never_mutates_state() {
    let (close_id, mut machine) = accepted_close();
    let mut slot = AssignedCloseSlot::create_for_assigned_owner();
    assert_accepted_id_unavailable(&slot, AssignedCloseSlotPhase::Vacant);
    slot.reserve().unwrap_or_else(|error| panic!("{error:?}"));
    assert_accepted_id_unavailable(&slot, AssignedCloseSlotPhase::Reserved);
    slot.observe_close_effect(AssignedConsumerEffect::AcceptClose { close_id })
        .unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(slot.accepted_id(), Ok(close_id));
    assert_eq!(slot.phase(), AssignedCloseSlotPhase::Accepted);

    let complete = machine
        .apply(AssignedConsumerInput::CloseDrained { close_id })
        .unwrap_or_else(|error| panic!("{error}"))
        .effects()[0];
    slot.observe_close_effect(complete)
        .unwrap_or_else(|error| panic!("{error:?}"));
    assert_accepted_id_unavailable(&slot, AssignedCloseSlotPhase::Ready);
    assert_eq!(slot.take_ready(), Ok(close_id));
    assert_accepted_id_unavailable(&slot, AssignedCloseSlotPhase::Reclaimed);
}

#[test]
fn out_of_order_and_foreign_effects_leave_reservation_untouched() {
    let (close_id, _) = accepted_close();
    let mut slot = AssignedCloseSlot::create_for_assigned_owner();

    assert_eq!(
        slot.observe_close_effect(AssignedConsumerEffect::AcceptClose { close_id }),
        Err(AssignedCloseSlotError::EffectOutOfOrder {
            effect: AssignedCloseEffectKind::Accept,
            close_id,
            phase: AssignedCloseSlotPhase::Vacant,
        })
    );
    slot.reserve().unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(
        slot.observe_close_effect(AssignedConsumerEffect::CompleteClose { close_id }),
        Err(AssignedCloseSlotError::EffectOutOfOrder {
            effect: AssignedCloseEffectKind::Complete,
            close_id,
            phase: AssignedCloseSlotPhase::Reserved,
        })
    );
    let foreign = revoke_effect();
    assert_eq!(
        slot.observe_close_effect(foreign),
        Err(AssignedCloseSlotError::UnexpectedEffect { effect: foreign })
    );
    assert_eq!(slot.phase(), AssignedCloseSlotPhase::Reserved);
}

#[test]
fn duplicate_and_stale_effects_never_replace_terminal_state() {
    let (close_id, mut machine) = accepted_close();
    let mut slot = AssignedCloseSlot::create_for_assigned_owner();
    slot.reserve().unwrap_or_else(|error| panic!("{error:?}"));
    slot.observe_close_effect(AssignedConsumerEffect::AcceptClose { close_id })
        .unwrap_or_else(|error| panic!("{error:?}"));

    assert_eq!(
        slot.observe_close_effect(AssignedConsumerEffect::AcceptClose { close_id }),
        Err(AssignedCloseSlotError::DuplicateEffect {
            effect: AssignedCloseEffectKind::Accept,
            close_id,
        })
    );
    let complete = machine
        .apply(AssignedConsumerInput::CloseDrained { close_id })
        .unwrap_or_else(|error| panic!("{error}"))
        .effects()[0];
    slot.observe_close_effect(complete)
        .unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(
        slot.observe_close_effect(complete),
        Err(AssignedCloseSlotError::DuplicateEffect {
            effect: AssignedCloseEffectKind::Complete,
            close_id,
        })
    );
    assert_eq!(slot.take_ready(), Ok(close_id));
    assert_eq!(
        slot.observe_close_effect(complete),
        Err(AssignedCloseSlotError::StaleEffect {
            effect: AssignedCloseEffectKind::Complete,
            close_id,
        })
    );
    assert_eq!(slot.phase(), AssignedCloseSlotPhase::Reclaimed);
}

fn accepted_close() -> (AssignedConsumerCloseId, AssignedConsumerMachine) {
    let mut machine = AssignedConsumerMachine::new();
    let transition = machine
        .apply(AssignedConsumerInput::BeginClose)
        .unwrap_or_else(|error| panic!("{error}"));
    let AssignedConsumerEffect::AcceptClose { close_id } = transition.effects()[0] else {
        panic!("close must accept before cleanup");
    };
    (close_id, machine)
}

fn assert_accepted_id_unavailable(slot: &AssignedCloseSlot, phase: AssignedCloseSlotPhase) {
    assert_eq!(
        slot.accepted_id(),
        Err(AssignedCloseSlotError::AcceptedIdUnavailable { phase })
    );
    assert_eq!(slot.phase(), phase);
}

fn revoke_effect() -> AssignedConsumerEffect {
    let mut machine = AssignedConsumerMachine::new();
    let offset = NextFetchOffset::try_from_raw(0).unwrap_or_else(|| panic!("valid offset"));
    machine
        .apply(AssignedConsumerInput::Assign {
            partitions: vec![AssignedPartition::new(
                AssignedTopicPartition::new(TopicId::from_raw(1), PartitionIndex::from_raw(0)),
                StartPosition::Offset(offset),
            )],
            now: Moment::from_tick(0),
            resolution_deadline: Deadline::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("{error}"));
    let close = machine
        .apply(AssignedConsumerInput::BeginClose)
        .unwrap_or_else(|error| panic!("{error}"));
    close
        .effects()
        .iter()
        .copied()
        .find(|effect| matches!(effect, AssignedConsumerEffect::Revoke { .. }))
        .unwrap_or_else(|| panic!("assigned close must revoke"))
}
