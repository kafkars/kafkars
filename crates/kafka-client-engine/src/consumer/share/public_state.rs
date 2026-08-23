//! Immediate public observation of share membership and assignment.

use std::sync::Arc;

use kafka_client_core::{GroupId, ShareGroupHeartbeatFailure};

use super::{
    port::ShareConsumerPort, public_registration::ShareConsumerHandle,
    registry::ShareConsumerRegistry, shard::ShareConsumerShardLockError,
};

/// One topic-partition currently assigned to a share member.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShareConsumerAssignmentPartition {
    topic: Arc<str>,
    partition: u32,
}

impl ShareConsumerAssignmentPartition {
    /// Returns the registered topic spelling.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the zero-based Kafka partition.
    pub const fn partition(&self) -> u32 {
        self.partition
    }
}

/// Atomically copied broker-confirmed share membership state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShareConsumerState {
    member_epoch: i32,
    assignment_generation: u64,
    partitions: Vec<ShareConsumerAssignmentPartition>,
}

impl ShareConsumerState {
    /// Returns the exact broker-issued positive member epoch.
    pub const fn member_epoch(&self) -> i32 {
        self.member_epoch
    }

    /// Returns the local nonzero assignment fence.
    pub const fn assignment_generation(&self) -> u64 {
        self.assignment_generation
    }

    /// Borrows the ordered current topic-partition assignment.
    pub fn partitions(&self) -> &[ShareConsumerAssignmentPartition] {
        &self.partitions
    }
}

/// Stable immediate-observation rejection category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShareConsumerStateErrorKind {
    /// Another owner temporarily holds the share registry.
    Contended,
    /// The registration has closed or no longer exists.
    Unavailable,
    /// The bounded assignment copy could not be allocated.
    Allocation,
    /// Registry or catalog ownership is inconsistent.
    Internal,
}

/// Immediate share-state observation failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShareConsumerStateError {
    kind: ShareConsumerStateErrorKind,
}

impl ShareConsumerStateError {
    /// Returns the stable observation category.
    pub const fn kind(self) -> ShareConsumerStateErrorKind {
        self.kind
    }
}

impl core::fmt::Display for ShareConsumerStateError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(formatter, "share state observation failed: {:?}", self.kind)
    }
}

impl std::error::Error for ShareConsumerStateError {}

impl ShareConsumerHandle {
    /// Returns the retained startup terminal, if membership failed before first success.
    #[doc(hidden)]
    pub fn startup_failure(&self) -> Option<ShareGroupHeartbeatFailure> {
        self.port
            .try_share_startup_failure(self.group_id)
            .ok()
            .flatten()
    }

    /// Copies the current broker-confirmed member epoch and assignment.
    pub fn state(&self) -> Result<Option<ShareConsumerState>, ShareConsumerStateError> {
        self.port
            .try_share_state(self.group_id)
            .map_err(|error| ShareConsumerStateError {
                kind: match error {
                    ShareConsumerStatePortError::Lock(ShareConsumerShardLockError::Contended) => {
                        ShareConsumerStateErrorKind::Contended
                    }
                    ShareConsumerStatePortError::Unknown => {
                        ShareConsumerStateErrorKind::Unavailable
                    }
                    ShareConsumerStatePortError::Allocation => {
                        ShareConsumerStateErrorKind::Allocation
                    }
                    ShareConsumerStatePortError::Lock(ShareConsumerShardLockError::Poisoned)
                    | ShareConsumerStatePortError::Invariant => {
                        ShareConsumerStateErrorKind::Internal
                    }
                },
            })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ShareConsumerStatePortError {
    Lock(ShareConsumerShardLockError),
    Unknown,
    Allocation,
    Invariant,
}

impl ShareConsumerPort {
    fn try_share_startup_failure(
        &self,
        group_id: GroupId,
    ) -> Result<Option<ShareGroupHeartbeatFailure>, ShareConsumerStatePortError> {
        let registry = self
            .shared
            .try_registry()
            .map_err(ShareConsumerStatePortError::Lock)?;
        registry.startup_failure(group_id)
    }

    fn try_share_state(
        &self,
        group_id: GroupId,
    ) -> Result<Option<ShareConsumerState>, ShareConsumerStatePortError> {
        let registry = self
            .shared
            .try_registry()
            .map_err(ShareConsumerStatePortError::Lock)?;
        registry.share_state(group_id)
    }
}

impl ShareConsumerRegistry {
    pub(super) fn startup_failure(
        &self,
        group_id: GroupId,
    ) -> Result<Option<ShareGroupHeartbeatFailure>, ShareConsumerStatePortError> {
        let entry = self
            .entry(group_id)
            .ok_or(ShareConsumerStatePortError::Unknown)?;
        Ok(entry.fault.or_else(|| {
            entry
                .membership
                .as_ref()
                .and_then(super::ShareMembershipInterpreter::startup_failure)
        }))
    }

    pub(super) fn share_state(
        &self,
        group_id: GroupId,
    ) -> Result<Option<ShareConsumerState>, ShareConsumerStatePortError> {
        let entry = self
            .entry(group_id)
            .ok_or(ShareConsumerStatePortError::Unknown)?;
        let Some(membership) = &entry.membership else {
            return Ok(None);
        };
        let (Some(epoch), Some(assignment)) = (
            membership.machine().member_epoch(),
            membership.activated_assignment(),
        ) else {
            return Ok(None);
        };
        let mut partitions = Vec::new();
        partitions
            .try_reserve_exact(assignment.partitions().len())
            .map_err(|_error| ShareConsumerStatePortError::Allocation)?;
        for assigned in assignment.partitions() {
            let topic = membership
                .catalog
                .topic_name(assigned.topic_id())
                .ok_or(ShareConsumerStatePortError::Invariant)?;
            partitions.push(ShareConsumerAssignmentPartition {
                topic: Arc::clone(topic),
                partition: assigned.partition().get(),
            });
        }
        Ok(Some(ShareConsumerState {
            member_epoch: epoch.get(),
            assignment_generation: assignment.assignment_generation().get(),
            partitions,
        }))
    }
}
