//! Shared fixtures for bounded group-registry scenarios.

use std::sync::Arc;

use kafka_client_core::{
    Deadline, GroupAssignmentPartition, GroupCheckpoint, GroupCheckpointEntry, GroupId,
    PartitionIndex, TopicId,
};

use crate::clock::OperationDeadline;

use super::{classic_group_test_support, registry::GroupConsumerRegistry};

pub(super) fn started_registry() -> GroupConsumerRegistry {
    GroupConsumerRegistry::start().unwrap_or_else(|error| panic!("registry start failed: {error}"))
}

pub(super) fn register(registry: &mut GroupConsumerRegistry, group: &str) -> GroupId {
    registry
        .try_register(
            Arc::from(group),
            vec![Arc::from("orders")],
            classic_group_test_support::timing(),
        )
        .unwrap_or_else(|failure| panic!("registration failed: {:?}", failure.kind))
}

pub(super) fn install_session(registry: &mut GroupConsumerRegistry, group_id: GroupId) {
    let entry = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("registered group expected"));
    let topic_id = entry
        .catalog
        .topic_id("orders")
        .unwrap_or_else(|| panic!("registered topic expected"));
    classic_group_test_support::install_follower(
        &mut entry.catalog,
        &mut entry.classic,
        "member-1",
        7,
        vec![GroupAssignmentPartition::new(
            topic_id,
            PartitionIndex::from_raw(0),
        )],
    );
}

pub(super) fn checkpoint(registry: &GroupConsumerRegistry, group_id: GroupId) -> GroupCheckpoint {
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("registered group expected"));
    let assignment = entry
        .catalog
        .live_assignment()
        .unwrap_or_else(|| panic!("live assignment expected"));
    let checkpoint_entry = GroupCheckpointEntry::try_new(
        TopicId::from_raw(1),
        PartitionIndex::from_raw(0),
        12,
        Some(4),
    )
    .unwrap_or_else(|error| panic!("checkpoint entry failed: {error}"));
    GroupCheckpoint::try_new(
        group_id,
        assignment.member_id(),
        assignment.assignment_generation(),
        vec![checkpoint_entry],
    )
    .unwrap_or_else(|error| panic!("checkpoint failed: {error}"))
}

pub(super) fn deadline(tick: u64) -> OperationDeadline {
    OperationDeadline::from_core_for_test(Deadline::from_tick(tick))
}

pub(super) fn stop_registry(registry: &mut GroupConsumerRegistry) {
    registry.close_admission();
    let join = registry
        .finish_shutdown()
        .unwrap_or_else(|error| panic!("finish shutdown failed: {error}"));
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("notifier join failed: {error}"));
}
