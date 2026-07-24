//! Core-owned one-partition pause, resume, and seek scenarios.

use std::time::Duration;

use kafka_client_core::{NextFetchOffset, StartPosition};

use super::{
    assigned_owner_effect::FrontEffect,
    assigned_owner_test::{input, owner},
};

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

#[test]
fn redundant_resume_commits_without_inventing_an_event_claim() {
    let mut owner = owner(1);
    let epoch = owner
        .replace_assignment(
            vec![input("orders", 0, StartPosition::Offset(offset(4)))],
            Duration::from_secs(10),
        )
        .unwrap_or_else(|error| panic!("assign: {error:?}"));
    assert_eq!(owner.interpret_front_effect(), FrontEffect::Interpreted);
    let partition = owner.topics.partitions()[0].partition();
    assert_eq!(owner.events.retained(), (1, 0));

    owner
        .resume(epoch, partition, Duration::from_secs(10))
        .unwrap_or_else(|error| panic!("redundant resume: {error:?}"));

    assert!(owner.effects.is_empty());
    assert_eq!(owner.events.retained(), (1, 0));
}

#[test]
fn paused_seek_commits_suspend_without_inventing_an_event_claim() {
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
    assert_eq!(owner.events.retained(), (0, 0));

    owner
        .seek(
            epoch,
            partition,
            StartPosition::Beginning,
            Duration::from_secs(10),
        )
        .unwrap_or_else(|error| panic!("paused seek: {error:?}"));
    drain(&mut owner);

    assert_eq!(owner.events.retained(), (0, 0));
}

fn drain(owner: &mut super::assigned_owner::AssignedConsumerOwner) {
    while !owner.effects.is_empty() {
        assert_eq!(owner.interpret_front_effect(), FrontEffect::Interpreted);
    }
}

fn offset(value: i64) -> NextFetchOffset {
    NextFetchOffset::try_from_raw(value).unwrap_or_else(|| panic!("nonnegative test offset"))
}
