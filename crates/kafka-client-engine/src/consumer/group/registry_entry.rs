//! One linear catalog entry and its admission lifecycle.

use std::sync::Arc;

use kafka_client_core::{ClassicGroupTiming, GroupId};

use super::{
    classic_group_execution::{ClassicGroupExecution, new_classic_group_execution},
    classic_group_owner::ClassicGroupOwner,
    session_catalog::{GroupSessionCatalog, GroupSessionCatalogError},
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
    pub(super) classic: ClassicGroupOwner,
    pub(super) execution: ClassicGroupExecution,
}

impl GroupConsumerEntry {
    pub(super) fn try_new(
        group_id: GroupId,
        group: &Arc<str>,
        local_topics: &[Arc<str>],
        timing: ClassicGroupTiming,
    ) -> Result<Self, GroupSessionCatalogError> {
        Ok(Self {
            state: GroupConsumerEntryState::Active,
            catalog: GroupSessionCatalog::try_new(group_id, Arc::clone(group), local_topics)?,
            classic: ClassicGroupOwner::new(group_id, timing),
            execution: new_classic_group_execution(),
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
}
