//! Call-boundary capture and lossless pre-core admission for group seek.

use std::time::Duration;

use crate::{
    clock::DeadlineCapture,
    consumer::{
        GroupConsumerHandle, GroupConsumerPartition,
        group::{
            GroupConsumerSeekPortError, GroupConsumerSeekRegistryError, GroupConsumerShardLockError,
        },
    },
};

use super::{GroupConsumerSeek, GroupConsumerSeekPosition};

const DEFAULT_GROUP_SEEK_TIMEOUT: Duration = Duration::from_secs(30);

/// Stable reason a group seek did not mutate deterministic position state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupConsumerSeekAdmissionErrorKind {
    /// Engine or seek admission has closed.
    Closed,
    /// Another owner currently holds the classic-group shard.
    Contended,
    /// The registered group no longer accepts this operation.
    GroupUnavailable,
    /// The synchronized engine host can no longer expose the group owner.
    HostUnavailable,
    /// The group has no active assignment.
    NoActiveAssignment,
    /// The topic-partition is absent from the active assignment.
    UnknownPartition,
    /// Earlier position work remains unsettled.
    Pending,
    /// Bounded event, effect, completion, or allocation capacity is unavailable.
    ResourceExhausted,
    /// The caller supplied an offset below zero.
    NegativeOffset,
    /// Internal ownership was inconsistent.
    InternalInvariant,
}

/// Pre-core rejection of one group seek.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GroupConsumerSeekAdmissionError {
    kind: GroupConsumerSeekAdmissionErrorKind,
}

impl GroupConsumerSeekAdmissionError {
    /// Returns the stable rejection category.
    pub const fn kind(&self) -> GroupConsumerSeekAdmissionErrorKind {
        self.kind
    }
}

impl core::fmt::Display for GroupConsumerSeekAdmissionError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(formatter, "classic-group seek rejected: {:?}", self.kind)
    }
}

impl std::error::Error for GroupConsumerSeekAdmissionError {}

/// One absolute seek boundary bound to the unique group handle.
#[must_use = "dropping abandons the captured deadline without admitting seek"]
pub struct GroupConsumerSeekCapture<'handle> {
    handle: &'handle mut GroupConsumerHandle,
    capture: DeadlineCapture,
}

impl GroupConsumerHandle {
    /// Captures the fixed seek deadline before facade input conversion.
    pub fn capture_seek(
        &mut self,
    ) -> Result<GroupConsumerSeekCapture<'_>, GroupConsumerSeekAdmissionError> {
        let capture = self
            .port
            .capture_seek_deadline(DEFAULT_GROUP_SEEK_TIMEOUT)
            .map_err(admission_error)?;
        Ok(GroupConsumerSeekCapture {
            handle: self,
            capture,
        })
    }
}

impl<'handle> GroupConsumerSeekCapture<'handle> {
    /// Attempts one fenced position replacement using this exact boundary.
    pub fn try_seek(
        self,
        partition: GroupConsumerPartition,
        position: GroupConsumerSeekPosition,
    ) -> Result<GroupConsumerSeek<'handle>, GroupConsumerSeekAdmissionError> {
        let Some(position) = position.try_into_core() else {
            return Err(GroupConsumerSeekAdmissionError {
                kind: GroupConsumerSeekAdmissionErrorKind::NegativeOffset,
            });
        };
        let admission = self
            .handle
            .port
            .try_seek_captured(self.handle.group_id, partition, position, self.capture)
            .map_err(admission_error)?;
        let wake_failed = admission.wake_failed();
        Ok(GroupConsumerSeek::new(
            self.handle,
            admission.completion,
            admission.registration,
            wake_failed,
        ))
    }
}

impl core::fmt::Debug for GroupConsumerSeekCapture<'_> {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("GroupConsumerSeekCapture")
            .finish_non_exhaustive()
    }
}

const fn admission_error(error: GroupConsumerSeekPortError) -> GroupConsumerSeekAdmissionError {
    let kind = match error {
        GroupConsumerSeekPortError::Closed => GroupConsumerSeekAdmissionErrorKind::Closed,
        GroupConsumerSeekPortError::Clock(_) | GroupConsumerSeekPortError::Notification => {
            GroupConsumerSeekAdmissionErrorKind::InternalInvariant
        }
        GroupConsumerSeekPortError::Lock(GroupConsumerShardLockError::Contended) => {
            GroupConsumerSeekAdmissionErrorKind::Contended
        }
        GroupConsumerSeekPortError::Lock(GroupConsumerShardLockError::Poisoned) => {
            GroupConsumerSeekAdmissionErrorKind::HostUnavailable
        }
        GroupConsumerSeekPortError::Registry(error) => match error {
            GroupConsumerSeekRegistryError::UnknownGroup
            | GroupConsumerSeekRegistryError::GroupUnavailable => {
                GroupConsumerSeekAdmissionErrorKind::GroupUnavailable
            }
            GroupConsumerSeekRegistryError::NoAssignment => {
                GroupConsumerSeekAdmissionErrorKind::NoActiveAssignment
            }
            GroupConsumerSeekRegistryError::UnknownPartition => {
                GroupConsumerSeekAdmissionErrorKind::UnknownPartition
            }
            GroupConsumerSeekRegistryError::Pending => GroupConsumerSeekAdmissionErrorKind::Pending,
            GroupConsumerSeekRegistryError::ResourceExhausted => {
                GroupConsumerSeekAdmissionErrorKind::ResourceExhausted
            }
            GroupConsumerSeekRegistryError::HostUnavailable => {
                GroupConsumerSeekAdmissionErrorKind::HostUnavailable
            }
            GroupConsumerSeekRegistryError::InternalInvariant => {
                GroupConsumerSeekAdmissionErrorKind::InternalInvariant
            }
        },
    };
    GroupConsumerSeekAdmissionError { kind }
}
