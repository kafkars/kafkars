//! Shared fixtures for private group offset commit host scenarios.

use std::sync::Arc;

use kafka_client_core::{
    AssignmentGeneration, Deadline, GroupCheckpoint, GroupCheckpointEntry, GroupId, PartitionIndex,
    TopicId,
};

use crate::{EngineConfig, clock::OperationDeadline, driver::DriverOwner};

use super::super::session_catalog::{GroupSessionCatalog, GroupSessionPartition};

pub(super) fn catalog() -> GroupSessionCatalog {
    let group_id =
        GroupId::try_from_raw(1).unwrap_or_else(|| panic!("group identity must be nonzero"));
    let generation = AssignmentGeneration::try_from_raw(1)
        .unwrap_or_else(|| panic!("assignment generation must be nonzero"));
    let mut catalog = GroupSessionCatalog::try_new(group_id, Arc::from("invoice-workers"))
        .unwrap_or_else(|error| panic!("catalog: {error:?}"));
    catalog
        .prepare_replacement(
            Arc::from("member-1"),
            7,
            generation,
            vec![GroupSessionPartition::new(
                Arc::from("orders"),
                PartitionIndex::from_raw(0),
            )],
        )
        .unwrap_or_else(|error| panic!("session: {error:?}"))
        .commit();
    catalog
}

pub(super) fn checkpoint(catalog: &GroupSessionCatalog) -> GroupCheckpoint {
    let assignment = catalog
        .live_assignment()
        .unwrap_or_else(|| panic!("live assignment"));
    let entry = GroupCheckpointEntry::try_new(
        TopicId::from_raw(1),
        PartitionIndex::from_raw(0),
        12,
        Some(4),
    )
    .unwrap_or_else(|error| panic!("checkpoint entry: {error}"));
    GroupCheckpoint::try_new(
        catalog.group_id(),
        assignment.member_id(),
        assignment.assignment_generation(),
        vec![entry],
    )
    .unwrap_or_else(|error| panic!("checkpoint: {error}"))
}

pub(super) fn deadline(tick: u64) -> OperationDeadline {
    OperationDeadline::from_core_for_test(Deadline::from_tick(tick))
}

pub(super) fn driver() -> DriverOwner {
    DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"))
}
