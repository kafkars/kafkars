//! Public incremental-assignment admission and stable-epoch scenarios.

use std::{sync::Arc, time::Duration};

use super::{
    AssignedConsumerAssignment, AssignedConsumerHandle, AssignedConsumerPartition,
    AssignedConsumerStartPosition, claim::AssignedConsumerClaimSlot, shard_test::setup,
};
use crate::consumer::assigned_owner_effect::FrontEffect;

#[test]
fn handle_adds_and_removes_with_new_control_revisions() {
    let (owner, port, wake) = setup();
    let mut handle = claim(port);
    let initial = handle
        .try_replace_assignment(vec![assignment("orders", 0)], Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("initial assignment: {error}"));
    drain(&owner);

    let added = handle
        .try_add_assignments(vec![assignment("orders", 1)], Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("add assignment: {error}"));
    let added_epoch = added.epoch().unwrap_or_else(|| panic!("addition epoch"));
    assert!(added_epoch > initial.epoch());
    drain(&owner);

    let removed = handle
        .try_remove_assignments(vec![target("orders", 1)])
        .unwrap_or_else(|error| panic!("remove assignment: {error}"));
    assert!(removed.epoch().is_some_and(|epoch| epoch > added_epoch));
    assert_eq!(wake.count(), 3);
}

#[test]
fn empty_unassigned_changes_are_accepted_without_epoch_or_wake() {
    let (_owner, port, wake) = setup();
    let mut handle = claim(port);

    let addition = handle
        .try_add_assignments(Vec::new(), Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("empty addition: {error}"));
    let removal = handle
        .try_remove_assignments(Vec::new())
        .unwrap_or_else(|error| panic!("empty removal: {error}"));

    assert_eq!(addition.epoch(), None);
    assert_eq!(removal.epoch(), None);
    assert_eq!(wake.count(), 0);
}

#[test]
fn empty_changes_preserve_pending_effects_without_an_extra_wake() {
    let (owner, port, wake) = setup();
    let mut handle = claim(port);
    let initial = handle
        .try_replace_assignment(vec![assignment("orders", 0)], Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("initial assignment: {error}"));

    let addition = handle
        .try_add_assignments(Vec::new(), Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("empty addition: {error}"));
    let removal = handle
        .try_remove_assignments(Vec::new())
        .unwrap_or_else(|error| panic!("empty removal: {error}"));

    assert_eq!(addition.epoch(), Some(initial.epoch()));
    assert_eq!(removal.epoch(), Some(initial.epoch()));
    assert_eq!(wake.count(), 1);
    owner
        .try_with_owner(|assigned| assert_eq!(assigned.effects.len(), 1))
        .unwrap_or_else(|error| panic!("inspect effects: {error:?}"));
}

fn claim(port: super::AssignedConsumerPort) -> AssignedConsumerHandle {
    let (slot, _closer) = AssignedConsumerClaimSlot::create_for_engine(port);
    let lifetime: Arc<dyn Send + Sync> = Arc::new(());
    slot.claim(lifetime)
        .unwrap_or_else(|error| panic!("claim assigned consumer: {error}"))
}

fn assignment(topic: &str, partition: i32) -> AssignedConsumerAssignment {
    AssignedConsumerAssignment::try_new(topic, partition, AssignedConsumerStartPosition::Offset(0))
        .unwrap_or_else(|error| panic!("valid assignment: {error}"))
}

fn target(topic: &str, partition: i32) -> AssignedConsumerPartition {
    AssignedConsumerPartition::try_new(topic, partition)
        .unwrap_or_else(|error| panic!("valid target: {error}"))
}

fn drain(owner: &super::AssignedConsumerShardOwner) {
    owner
        .try_with_owner(|assigned| {
            while !assigned.effects.is_empty() {
                assert_eq!(assigned.interpret_front_effect(), FrontEffect::Interpreted);
            }
        })
        .unwrap_or_else(|error| panic!("drain effects: {error:?}"));
}
