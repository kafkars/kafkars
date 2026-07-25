//! One linear catalog entry and its admission lifecycle.

use std::sync::Arc;

use kafka_client_core::{AssignmentGeneration, GroupId};

use super::{
    session_catalog::{GroupSessionCatalog, GroupSessionCatalogError, GroupSessionPartition},
    session_catalog_prepared::PreparedGroupSessionReplacement,
};

/// Whether one retained group can still admit new operations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum GroupConsumerEntryState {
    Active,
    Closing,
}

/// One bounded group spelling, session catalog, and close fence.
pub(super) struct GroupConsumerEntry {
    pub(super) state: GroupConsumerEntryState,
    pub(super) catalog: GroupSessionCatalog,
}

impl GroupConsumerEntry {
    pub(super) fn try_new(
        group_id: GroupId,
        group: Arc<str>,
    ) -> Result<Self, GroupSessionCatalogError> {
        Ok(Self {
            state: GroupConsumerEntryState::Active,
            catalog: GroupSessionCatalog::try_new(group_id, group)?,
        })
    }

    pub(super) const fn group_id(&self) -> GroupId {
        self.catalog.group_id()
    }

    pub(super) fn group_bytes(&self) -> usize {
        self.catalog.group().len()
    }

    pub(super) const fn is_active(&self) -> bool {
        matches!(self.state, GroupConsumerEntryState::Active)
    }

    pub(super) fn prepare_replacement(
        &mut self,
        member: Arc<str>,
        classic_generation: i32,
        assignment_generation: AssignmentGeneration,
        partitions: Vec<GroupSessionPartition>,
    ) -> Result<PreparedGroupSessionReplacement<'_>, GroupConsumerEntrySessionError> {
        if !self.is_active() {
            return Err(GroupConsumerEntrySessionError::Closing);
        }
        let catalog = &mut self.catalog;
        catalog
            .prepare_replacement(
                member,
                classic_generation,
                assignment_generation,
                partitions,
            )
            .map_err(GroupConsumerEntrySessionError::Catalog)
    }
}

/// Session replacement failure preserving the entry lifecycle decision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum GroupConsumerEntrySessionError {
    Closing,
    Catalog(GroupSessionCatalogError),
}
