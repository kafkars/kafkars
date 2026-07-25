//! Registry-selected session replacement without exposing mutable catalogs.

use std::sync::Arc;

use kafka_client_core::{AssignmentGeneration, GroupId};

use super::{
    registry::GroupConsumerRegistry, registry_entry::GroupConsumerEntrySessionError,
    session_catalog::GroupSessionPartition,
    session_catalog_prepared::PreparedGroupSessionReplacement,
};

/// Registry selection or entry-local session staging failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum GroupConsumerSessionFailure {
    UnknownGroup,
    Entry(GroupConsumerEntrySessionError),
}

impl GroupConsumerRegistry {
    pub(super) fn prepare_session_replacement(
        &mut self,
        group_id: GroupId,
        member: Arc<str>,
        classic_generation: i32,
        assignment_generation: AssignmentGeneration,
        partitions: Vec<GroupSessionPartition>,
    ) -> Result<PreparedGroupSessionReplacement<'_>, GroupConsumerSessionFailure> {
        let entry = self
            .entries
            .iter_mut()
            .find(|entry| entry.group_id() == group_id)
            .ok_or(GroupConsumerSessionFailure::UnknownGroup)?;
        entry
            .prepare_replacement(
                member,
                classic_generation,
                assignment_generation,
                partitions,
            )
            .map_err(GroupConsumerSessionFailure::Entry)
    }
}
