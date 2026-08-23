//! Bounded ownership of share-coordinator route invalidation capabilities.
#![allow(
    dead_code,
    reason = "the bounded share invalidation owner precedes its hosted registry checkpoint"
)]

use kafka_client_core::GroupId;
use kafka_driver::{
    Call, CompletionError, InvalidationDisposition, RouteFailureToken, SubmitError,
};

/// Exact route capability transferred from one failed share heartbeat.
#[must_use = "a transferred share-coordinator token must be installed or discarded"]
pub(crate) struct PendingShareCoordinatorInvalidation {
    group_id: GroupId,
    route_token: RouteFailureToken,
}

impl PendingShareCoordinatorInvalidation {
    pub(crate) const fn new(group_id: GroupId, route_token: RouteFailureToken) -> Self {
        Self {
            group_id,
            route_token,
        }
    }

    pub(crate) const fn group_id(&self) -> GroupId {
        self.group_id
    }

    pub(super) fn into_parts(self) -> (GroupId, RouteFailureToken) {
        (self.group_id, self.route_token)
    }
}

pub(super) enum ShareCoordinatorInvalidationState {
    Queued(PendingShareCoordinatorInvalidation),
    Active {
        group_id: GroupId,
        call: Call<InvalidationDisposition>,
    },
}

impl ShareCoordinatorInvalidationState {
    pub(super) const fn group_id(&self) -> GroupId {
        match self {
            Self::Queued(pending) => pending.group_id(),
            Self::Active { group_id, .. } => *group_id,
        }
    }
}

/// Preflighted slot for one exact share member's invalidation capability.
#[must_use = "a share invalidation reservation must install its matching capability"]
pub(crate) struct ShareCoordinatorInvalidationPermit<'owner> {
    group_id: GroupId,
    entries: &'owner mut Vec<ShareCoordinatorInvalidationState>,
}

impl ShareCoordinatorInvalidationPermit<'_> {
    pub(crate) fn install(
        self,
        pending: PendingShareCoordinatorInvalidation,
    ) -> Result<(), PendingShareCoordinatorInvalidation> {
        if pending.group_id() != self.group_id {
            return Err(pending);
        }
        self.entries
            .push(ShareCoordinatorInvalidationState::Queued(pending));
        Ok(())
    }
}

/// Capacity-bounded queued and driver-accepted share invalidations.
pub(crate) struct ShareCoordinatorInvalidations {
    capacity: usize,
    pub(super) entries: Vec<ShareCoordinatorInvalidationState>,
}

impl ShareCoordinatorInvalidations {
    pub(crate) fn try_new(capacity: usize) -> Result<Self, std::collections::TryReserveError> {
        let mut entries = Vec::new();
        entries.try_reserve_exact(capacity)?;
        Ok(Self { capacity, entries })
    }

    pub(crate) fn try_reserve(
        &mut self,
        group_id: GroupId,
    ) -> Result<ShareCoordinatorInvalidationPermit<'_>, ShareCoordinatorInvalidationReserveError>
    {
        if self.blocks_submission(group_id) {
            return Err(ShareCoordinatorInvalidationReserveError::Duplicate { group_id });
        }
        if self.entries.len() >= self.capacity {
            return Err(ShareCoordinatorInvalidationReserveError::Capacity {
                limit: self.capacity,
            });
        }
        Ok(ShareCoordinatorInvalidationPermit {
            group_id,
            entries: &mut self.entries,
        })
    }

    pub(crate) fn blocks_submission(&self, group_id: GroupId) -> bool {
        self.entries
            .iter()
            .any(|entry| entry.group_id() == group_id)
    }

    pub(crate) fn retained_count(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn discard_queued(&mut self, group_id: GroupId) -> bool {
        let Some(index) = self.entries.iter().position(|entry| {
            matches!(entry, ShareCoordinatorInvalidationState::Queued(pending) if pending.group_id() == group_id)
        }) else {
            return false;
        };
        drop(self.entries.remove(index));
        true
    }

    pub(crate) fn discard_after_driver_shutdown(&mut self) {
        self.entries.clear();
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ShareCoordinatorInvalidationReserveError {
    Capacity { limit: usize },
    Duplicate { group_id: GroupId },
}

#[must_use = "driver rejection leaves the exact share invalidation queued"]
pub(crate) struct ShareCoordinatorInvalidationAdmissionFailure {
    group_id: GroupId,
    source: SubmitError,
}

impl ShareCoordinatorInvalidationAdmissionFailure {
    pub(super) const fn new(group_id: GroupId, source: SubmitError) -> Self {
        Self { group_id, source }
    }

    pub(crate) const fn group_id(&self) -> GroupId {
        self.group_id
    }

    #[allow(
        unreachable_patterns,
        reason = "the published driver RC is non-exhaustive while this boundary is fail-closed"
    )]
    pub(crate) const fn kind(&self) -> ShareCoordinatorInvalidationAdmissionFailureKind {
        match &self.source {
            SubmitError::Full => ShareCoordinatorInvalidationAdmissionFailureKind::Full,
            SubmitError::Closed
            | SubmitError::Wake(_)
            | SubmitError::IdentityExhausted
            | SubmitError::ForeignDriver
            | SubmitError::VersionBoundsInvalid { .. } => {
                ShareCoordinatorInvalidationAdmissionFailureKind::Terminal
            }
            _ => ShareCoordinatorInvalidationAdmissionFailureKind::Terminal,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ShareCoordinatorInvalidationAdmissionFailureKind {
    Full,
    Terminal,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ShareCoordinatorInvalidationPermission {
    Applied,
    IgnoredStale,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ShareCoordinatorInvalidationTerminalFailure {
    Unavailable,
    CapacityReached,
    Completion(CompletionError),
    UnrecognizedDisposition,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ShareCoordinatorInvalidationPoll {
    Idle,
    Submitted {
        group_id: GroupId,
    },
    Pending {
        group_id: GroupId,
    },
    Terminal {
        group_id: GroupId,
        result: Result<
            ShareCoordinatorInvalidationPermission,
            ShareCoordinatorInvalidationTerminalFailure,
        >,
    },
}
