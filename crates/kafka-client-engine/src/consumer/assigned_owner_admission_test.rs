//! Two-phase assignment and close-admission scenarios.

use std::time::Duration;

use kafka_client_core::{AssignedConsumerEffect, NextFetchOffset, StartPosition};

use super::{
    assigned_close_error::AssignedCloseSlotPhase,
    assigned_owner_effect::FrontEffect,
    assigned_owner_model::AssignedConsumerOwnerError,
    assigned_owner_test::{input, owner},
};

#[test]
fn assignment_commits_topics_only_after_core_accepts() {
    let mut owner = owner(2);
    let epoch = owner
        .replace_assignment(
            vec![input("orders", 0, StartPosition::Offset(offset(4)))],
            Duration::from_secs(10),
        )
        .unwrap_or_else(|error| panic!("assign: {error:?}"));

    assert_eq!(owner.machine.assignment_epoch(), Some(epoch));
    assert_eq!(owner.topics.retained_topic_count(), 1);
    assert!(matches!(
        owner.effects.front(),
        Some(AssignedConsumerEffect::FetchReady { .. })
    ));
}

#[test]
fn close_reserves_before_core_acceptance_and_rolls_back_core_rejection() {
    let mut accepted = owner(1);
    accepted
        .begin_close()
        .unwrap_or_else(|error| panic!("begin close: {error:?}"));
    assert_eq!(accepted.close.phase(), AssignedCloseSlotPhase::Reserved);

    let mut rejected = owner(1);
    rejected
        .machine
        .apply(kafka_client_core::AssignedConsumerInput::BeginClose)
        .unwrap_or_else(|error| panic!("close core fixture: {error}"));
    assert!(matches!(
        rejected.begin_close(),
        Err(AssignedConsumerOwnerError::Core(
            kafka_client_core::AssignedConsumerMachineError::ConsumerClosed
        ))
    ));
    assert_eq!(rejected.close.phase(), AssignedCloseSlotPhase::Vacant);
}

#[test]
fn rejected_duplicate_assignment_does_not_commit_staged_names() {
    let mut owner = owner(2);
    let result = owner.replace_assignment(
        vec![
            input("orders", 0, StartPosition::Beginning),
            input("orders", 0, StartPosition::End),
        ],
        Duration::from_secs(10),
    );

    assert!(matches!(result, Err(AssignedConsumerOwnerError::Core(_))));
    assert_eq!(owner.topics.retained_topic_count(), 0);
    assert!(owner.effects.is_empty());
}

#[test]
fn pause_resume_and_seek_remain_core_owned_controls() {
    let mut owner = owner(1);
    let epoch = owner
        .replace_assignment(
            vec![input("orders", 0, StartPosition::Offset(offset(4)))],
            Duration::from_secs(10),
        )
        .unwrap_or_else(|error| panic!("assign: {error:?}"));
    assert_eq!(owner.interpret_front_effect(), FrontEffect::Interpreted);
    let partition = owner.topics.partitions()[0].partition();

    owner
        .pause(epoch, partition)
        .unwrap_or_else(|error| panic!("pause: {error:?}"));
    drain(&mut owner);
    owner
        .resume(epoch, partition, Duration::from_secs(10))
        .unwrap_or_else(|error| panic!("resume: {error:?}"));
    drain(&mut owner);
    owner
        .seek(
            epoch,
            partition,
            StartPosition::Beginning,
            Duration::from_secs(10),
        )
        .unwrap_or_else(|error| panic!("seek: {error:?}"));
    drain(&mut owner);
    assert_eq!(owner.pending_positions.len(), 1);
}

fn drain(owner: &mut super::assigned_owner::AssignedConsumerOwner) {
    while !owner.effects.is_empty() {
        assert_eq!(owner.interpret_front_effect(), FrontEffect::Interpreted);
    }
}

fn offset(value: i64) -> NextFetchOffset {
    NextFetchOffset::try_from_raw(value).unwrap_or_else(|| panic!("nonnegative test offset"))
}
