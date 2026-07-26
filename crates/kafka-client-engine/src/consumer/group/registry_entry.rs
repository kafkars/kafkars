//! One linear catalog entry and its admission lifecycle.

use std::sync::Arc;

use kafka_client_core::{ClassicGroupTiming, ClassicHeartbeatPolicy, ClassicRejoinPolicy, GroupId};

use super::{
    classic_group_entry_fault::ClassicGroupEntryFault,
    classic_group_execution::{ClassicGroupExecution, new_classic_group_execution},
    classic_group_fetch::{ClassicGroupFetchBuildError, ClassicGroupFetchOwner},
    classic_group_heartbeat::ClassicHeartbeatExecution,
    classic_group_owner::ClassicGroupOwner,
    classic_group_position::ClassicGroupPositionExecution,
    classic_group_rediscovery::ClassicCoordinatorRediscovery,
    classic_group_rejoin::ClassicGroupRejoinExecution,
    session_catalog::{GroupSessionCatalog, GroupSessionCatalogError},
};

/// Whether one retained group can still admit new operations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum GroupConsumerEntryState {
    Active,
    Closing,
}

/// Truthful local construction source before one registry entry exists.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum GroupConsumerEntryBuildError {
    Catalog(GroupSessionCatalogError),
    Fetch(ClassicGroupFetchBuildError),
}

/// One bounded group spelling, session catalog, and close fence.
pub(super) struct GroupConsumerEntry {
    pub(super) state: GroupConsumerEntryState,
    pub(super) catalog: GroupSessionCatalog,
    pub(super) classic: ClassicGroupOwner,
    pub(super) execution: ClassicGroupExecution,
    pub(super) fetch: ClassicGroupFetchOwner,
    pub(super) heartbeat: ClassicHeartbeatExecution,
    pub(super) position: ClassicGroupPositionExecution,
    pub(super) rejoin: ClassicGroupRejoinExecution,
    pub(super) rediscovery: ClassicCoordinatorRediscovery,
    pub(super) fault: Option<ClassicGroupEntryFault>,
}

impl GroupConsumerEntry {
    pub(super) fn try_new(
        group_id: GroupId,
        group: &Arc<str>,
        local_topics: &[Arc<str>],
        timing: ClassicGroupTiming,
        heartbeat_policy: ClassicHeartbeatPolicy,
        rejoin_policy: ClassicRejoinPolicy,
    ) -> Result<Self, GroupConsumerEntryBuildError> {
        Ok(Self {
            state: GroupConsumerEntryState::Active,
            catalog: GroupSessionCatalog::try_new(group_id, Arc::clone(group), local_topics)
                .map_err(GroupConsumerEntryBuildError::Catalog)?,
            classic: ClassicGroupOwner::new(group_id, timing, heartbeat_policy, rejoin_policy),
            execution: new_classic_group_execution(),
            fetch: ClassicGroupFetchOwner::try_new()
                .map_err(GroupConsumerEntryBuildError::Fetch)?,
            heartbeat: ClassicHeartbeatExecution::new(),
            position: ClassicGroupPositionExecution::new(),
            rejoin: ClassicGroupRejoinExecution::new(),
            rediscovery: ClassicCoordinatorRediscovery::new(),
            fault: None,
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
