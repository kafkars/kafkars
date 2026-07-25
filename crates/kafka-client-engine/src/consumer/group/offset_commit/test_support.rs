//! Shared fixtures for private group offset commit host scenarios.

use std::sync::Arc;

use kafka_client_core::{
    Deadline, GroupAssignmentPartition, GroupCheckpoint, GroupCheckpointEntry, GroupId,
    PartitionIndex, TopicId,
};

use crate::{EngineConfig, clock::OperationDeadline, driver::DriverOwner};

use super::super::{
    classic_group_owner::ClassicGroupOwner, classic_group_test_support,
    session_catalog::GroupSessionCatalog,
};

pub(super) fn catalog() -> GroupSessionCatalog {
    let group_id =
        GroupId::try_from_raw(1).unwrap_or_else(|| panic!("group identity must be nonzero"));
    let mut catalog = GroupSessionCatalog::try_new(
        group_id,
        Arc::from("invoice-workers"),
        &[Arc::from("orders")],
    )
    .unwrap_or_else(|error| panic!("catalog: {error:?}"));
    let mut owner = ClassicGroupOwner::new(
        group_id,
        classic_group_test_support::timing(),
        classic_group_test_support::heartbeat_policy(),
    );
    classic_group_test_support::install_follower(
        &mut catalog,
        &mut owner,
        "member-1",
        7,
        vec![GroupAssignmentPartition::new(
            TopicId::from_raw(1),
            PartitionIndex::from_raw(0),
        )],
    );
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
