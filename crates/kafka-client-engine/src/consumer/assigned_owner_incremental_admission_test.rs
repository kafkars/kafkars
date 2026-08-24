//! Atomic incremental assignment, stable-fence, and rejection scenarios.

use std::time::Duration;

use kafka_client_core::{AssignedConsumerEffect, FetchOwnership, NextFetchOffset, StartPosition};

use super::{
    assigned_host::AssignedConsumerPartition,
    assigned_owner_effect::FrontEffect,
    assigned_owner_model::AssignedConsumerOwnerError,
    assigned_owner_test::{input, owner},
};

#[test]
fn addition_preserves_survivor_fence_claim_and_catalog_order() {
    let mut owner = owner(3);
    let initial_epoch = owner
        .replace_assignment(
            vec![input("orders", 0, StartPosition::Offset(offset(4)))],
            Duration::from_secs(10),
        )
        .unwrap_or_else(|error| panic!("initial assignment: {error:?}"));
    let survivor = match owner.effects.pop_front() {
        Some(AssignedConsumerEffect::FetchReady { fence, .. }) => fence,
        effect => panic!("survivor Fetch, got {effect:?}"),
    };

    let changed_epoch = owner
        .add_assignments(
            vec![input("payments", 1, StartPosition::Offset(offset(8)))],
            Duration::from_secs(10),
        )
        .unwrap_or_else(|error| panic!("add assignment: {error:?}"))
        .unwrap_or_else(|| panic!("nonempty addition epoch"));

    assert!(changed_epoch > initial_epoch);
    assert_eq!(
        owner.machine.fetch_ownership(survivor),
        Ok(FetchOwnership::Active)
    );
    assert_eq!(owner.events.retained(), (2, 0));
    assert_eq!(owner.effects.len(), 1);
    assert!(matches!(
        owner.effects.front(),
        Some(AssignedConsumerEffect::FetchReady { fence, .. })
            if fence.position().partition() != survivor.position().partition()
    ));
    let names = owner
        .topics
        .partitions()
        .iter()
        .map(|partition| {
            owner
                .topics
                .name(partition.partition().topic_id())
                .map_or_else(|error| panic!("topic name: {error:?}"), AsRef::as_ref)
        })
        .collect::<Vec<_>>();
    assert_eq!(names, vec!["orders", "payments"]);
}

#[test]
fn removal_revokes_only_target_and_preserves_survivor_work() {
    let mut owner = owner(3);
    let initial_epoch = owner
        .replace_assignment(
            vec![
                input("orders", 0, StartPosition::Offset(offset(4))),
                input("orders", 1, StartPosition::Offset(offset(8))),
            ],
            Duration::from_secs(10),
        )
        .unwrap_or_else(|error| panic!("initial assignment: {error:?}"));
    let first = fetch(&mut owner, "first");
    let removed = fetch(&mut owner, "second");

    let changed_epoch = owner
        .remove_assignments(&[target("orders", 1)])
        .unwrap_or_else(|error| panic!("remove assignment: {error:?}"))
        .unwrap_or_else(|| panic!("nonempty removal epoch"));

    assert!(changed_epoch > initial_epoch);
    assert_eq!(
        owner.machine.fetch_ownership(first),
        Ok(FetchOwnership::Active)
    );
    assert_eq!(
        owner.machine.fetch_ownership(removed),
        Ok(FetchOwnership::Superseded)
    );
    assert_eq!(owner.topics.partitions().len(), 1);
    assert_eq!(
        owner.topics.partitions()[0].partition(),
        first.position().partition()
    );
    assert_eq!(owner.events.retained(), (2, 0));
    assert!(matches!(
        owner.effects.front(),
        Some(AssignedConsumerEffect::Revoke { assignment_epoch, partition })
            if *assignment_epoch == removed.position().assignment_epoch()
                && *partition == removed.position().partition()
    ));
    assert_eq!(owner.interpret_front_effect(), FrontEffect::Interpreted);
    assert_eq!(owner.events.retained(), (1, 0));
}

#[test]
fn rejected_incremental_changes_leave_catalog_claims_and_epoch_exact() {
    let mut owner = owner(3);
    let epoch = owner
        .replace_assignment(
            vec![input("orders", 0, StartPosition::Offset(offset(4)))],
            Duration::from_secs(10),
        )
        .unwrap_or_else(|error| panic!("initial assignment: {error:?}"));
    owner.effects.clear();

    assert!(matches!(
        owner.add_assignments(
            vec![
                input("payments", 1, StartPosition::Offset(offset(8))),
                input("orders", 0, StartPosition::Offset(offset(9))),
            ],
            Duration::from_secs(10),
        ),
        Err(AssignedConsumerOwnerError::Core(
            kafka_client_core::AssignedConsumerMachineError::PartitionAlreadyAssigned { .. }
        ))
    ));
    assert_eq!(owner.machine.assignment_epoch(), Some(epoch));
    assert_eq!(owner.topics.retained_topic_count(), 1);
    assert_eq!(owner.events.retained(), (1, 0));

    assert!(matches!(
        owner.remove_assignments(&[target("orders", 2)]),
        Err(AssignedConsumerOwnerError::Core(
            kafka_client_core::AssignedConsumerMachineError::UnknownPartition { .. }
        ))
    ));
    assert_eq!(owner.machine.assignment_epoch(), Some(epoch));
    assert_eq!(owner.topics.partitions().len(), 1);
    assert_eq!(owner.events.retained(), (1, 0));
}

#[test]
fn empty_changes_preserve_unassigned_and_assigned_epochs() {
    let mut owner = owner(1);
    assert_eq!(
        owner
            .add_assignments(Vec::new(), Duration::from_secs(10))
            .unwrap_or_else(|error| panic!("empty addition: {error:?}")),
        None
    );
    assert_eq!(
        owner
            .remove_assignments(&[])
            .unwrap_or_else(|error| panic!("empty removal: {error:?}")),
        None
    );
    let epoch = owner
        .replace_assignment(
            vec![input("orders", 0, StartPosition::Offset(offset(4)))],
            Duration::from_secs(10),
        )
        .unwrap_or_else(|error| panic!("initial assignment: {error:?}"));
    let pending_fence = match owner.effects.front() {
        Some(AssignedConsumerEffect::FetchReady { fence, .. }) => *fence,
        effect => panic!("pending survivor Fetch, got {effect:?}"),
    };
    assert_eq!(
        owner
            .add_assignments(Vec::new(), Duration::from_secs(10))
            .unwrap_or_else(|error| panic!("assigned empty addition: {error:?}")),
        Some(epoch)
    );
    assert_eq!(
        owner
            .remove_assignments(&[])
            .unwrap_or_else(|error| panic!("assigned empty removal: {error:?}")),
        Some(epoch)
    );
    assert_eq!(owner.effects.len(), 1);
    assert!(matches!(
        owner.effects.front(),
        Some(AssignedConsumerEffect::FetchReady { fence, .. }) if *fence == pending_fence
    ));
}

fn fetch(
    owner: &mut super::assigned_owner::AssignedConsumerOwner,
    label: &str,
) -> kafka_client_core::FetchFence {
    match owner.effects.pop_front() {
        Some(AssignedConsumerEffect::FetchReady { fence, .. }) => fence,
        effect => panic!("{label} Fetch, got {effect:?}"),
    }
}

fn target(topic: &str, partition: i32) -> AssignedConsumerPartition {
    AssignedConsumerPartition::try_new(topic, partition)
        .unwrap_or_else(|error| panic!("removal target: {error}"))
}

fn offset(value: i64) -> NextFetchOffset {
    NextFetchOffset::try_from_raw(value).unwrap_or_else(|| panic!("nonnegative test offset"))
}
