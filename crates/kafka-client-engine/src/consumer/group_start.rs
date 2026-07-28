//! Capture-first public result vocabulary for classic-group membership start.

use std::time::Duration;

use super::{GroupConsumerCycleAdmission, GroupConsumerCyclePortErrorCategory, GroupConsumerPort};
use crate::clock::DeadlineCapture;

/// Linear membership-start deadline captured before higher-layer input work.
#[must_use = "a captured group start should be admitted or deliberately discarded"]
pub struct GroupConsumerStartCapture {
    port: GroupConsumerPort,
    capture: DeadlineCapture,
}

impl GroupConsumerStartCapture {
    pub(in crate::consumer) fn capture(
        port: GroupConsumerPort,
        timeout: Duration,
    ) -> Result<Self, GroupConsumerStartError> {
        let capture = port.capture_cycle_deadline(timeout).map_err(|_error| {
            GroupConsumerStartError::from_port(GroupConsumerCyclePortErrorCategory::InvalidTimeout)
        })?;
        Ok(Self { port, capture })
    }

    pub(in crate::consumer) fn admit(
        self,
        port: &GroupConsumerPort,
        group_id: kafka_client_core::GroupId,
    ) -> Result<GroupConsumerCycleAdmission, GroupConsumerStartError> {
        if !self.port.shares_registry_with(port) {
            return Err(GroupConsumerStartError::from_port(
                GroupConsumerCyclePortErrorCategory::InternalInvariant,
            ));
        }
        port.admit_captured_cycle(group_id, self.capture)
            .map_err(|error| GroupConsumerStartError::from_port(error.public_category()))
    }
}

impl core::fmt::Debug for GroupConsumerStartCapture {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("GroupConsumerStartCapture")
            .finish_non_exhaustive()
    }
}

/// Stable reason a membership cycle was rejected before deterministic core mutation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupConsumerStartErrorKind {
    /// Engine or group admission has closed.
    Closed,
    /// Another owner currently holds the bounded group registry.
    Contended,
    /// This registered group already owns membership work.
    AlreadyStarted,
    /// The exact registered group is closing or no longer available.
    GroupUnavailable,
    /// The requested timeout cannot be represented as an absolute deadline.
    InvalidTimeout,
    /// An engine ownership invariant prevented admission.
    Internal,
}

/// Pre-core membership-start rejection that leaves the handle retryable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GroupConsumerStartError {
    kind: GroupConsumerStartErrorKind,
}

impl GroupConsumerStartError {
    pub(in crate::consumer) const fn from_port(
        category: GroupConsumerCyclePortErrorCategory,
    ) -> Self {
        let kind = match category {
            GroupConsumerCyclePortErrorCategory::InvalidTimeout => {
                GroupConsumerStartErrorKind::InvalidTimeout
            }
            GroupConsumerCyclePortErrorCategory::Closed => GroupConsumerStartErrorKind::Closed,
            GroupConsumerCyclePortErrorCategory::Contended => {
                GroupConsumerStartErrorKind::Contended
            }
            GroupConsumerCyclePortErrorCategory::AlreadyStarted => {
                GroupConsumerStartErrorKind::AlreadyStarted
            }
            GroupConsumerCyclePortErrorCategory::GroupUnavailable => {
                GroupConsumerStartErrorKind::GroupUnavailable
            }
            GroupConsumerCyclePortErrorCategory::InternalInvariant => {
                GroupConsumerStartErrorKind::Internal
            }
        };
        Self { kind }
    }

    /// Returns the stable rejection category.
    pub const fn kind(self) -> GroupConsumerStartErrorKind {
        self.kind
    }
}

impl core::fmt::Display for GroupConsumerStartError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            formatter,
            "classic-group membership start rejected: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for GroupConsumerStartError {}

/// Accepted membership ownership plus advisory engine degradation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use = "accepted membership remains owned even when advisory progress degraded"]
pub struct GroupConsumerStartAccepted {
    entry_faulted: bool,
    wake_failed: bool,
}

impl GroupConsumerStartAccepted {
    pub(in crate::consumer) const fn new(entry_faulted: bool, wake_failed: bool) -> Self {
        Self {
            entry_faulted,
            wake_failed,
        }
    }

    /// Reports that accepted post-core ownership was frozen by an invariant fault.
    pub const fn entry_faulted(self) -> bool {
        self.entry_faulted
    }

    /// Reports that the advisory reactor wake failed after admission.
    pub const fn wake_failed(self) -> bool {
        self.wake_failed
    }
}
