//! Bounded ownership of core-authorized classic-group coordinator invalidations.

use kafka_client_core::GroupId;
use kafka_driver::{Call, CompletionError, InvalidationDisposition, RouteFailureToken};

/// Opaque coordinator capability transferred from one exact membership call.
#[must_use = "a transferred coordinator token must enter bounded invalidation ownership"]
pub(crate) struct PendingClassicCoordinatorInvalidation {
    group_id: GroupId,
    route_token: RouteFailureToken,
}

impl PendingClassicCoordinatorInvalidation {
    pub(super) const fn new(group_id: GroupId, route_token: RouteFailureToken) -> Self {
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

pub(super) enum ClassicCoordinatorInvalidationState {
    Queued(PendingClassicCoordinatorInvalidation),
    Active {
        group_id: GroupId,
        call: Call<InvalidationDisposition>,
    },
    CompletionFailed {
        group_id: GroupId,
        source: CompletionError,
    },
}

impl ClassicCoordinatorInvalidationState {
    pub(super) const fn group_id(&self) -> GroupId {
        match self {
            Self::Queued(pending) => pending.group_id(),
            Self::Active { group_id, .. } | Self::CompletionFailed { group_id, .. } => *group_id,
        }
    }
}

/// Preflighted slot for one exact group's transferred invalidation capability.
#[must_use = "an invalidation reservation must install its matching pending capability"]
pub(crate) struct ClassicCoordinatorInvalidationPermit<'a> {
    group_id: GroupId,
    entries: &'a mut Vec<ClassicCoordinatorInvalidationState>,
}

impl ClassicCoordinatorInvalidationPermit<'_> {
    pub(crate) const fn group_id(&self) -> GroupId {
        self.group_id
    }

    pub(crate) fn install(
        self,
        pending: PendingClassicCoordinatorInvalidation,
    ) -> Result<(), ClassicCoordinatorInvalidationInstallFailure> {
        if pending.group_id() != self.group_id {
            return Err(ClassicCoordinatorInvalidationInstallFailure {
                expected: self.group_id,
                pending,
            });
        }
        self.entries
            .push(ClassicCoordinatorInvalidationState::Queued(pending));
        Ok(())
    }
}

/// Capacity-bounded owner for queued and driver-accepted coordinator invalidations.
pub(crate) struct ClassicCoordinatorInvalidations {
    capacity: usize,
    pub(super) entries: Vec<ClassicCoordinatorInvalidationState>,
}

impl ClassicCoordinatorInvalidations {
    #[cfg(test)]
    pub(crate) fn new(capacity: usize) -> Self {
        Self::try_new(capacity)
            .unwrap_or_else(|_error| panic!("test invalidation reservation failed"))
    }

    pub(crate) fn try_new(capacity: usize) -> Result<Self, std::collections::TryReserveError> {
        let mut entries = Vec::new();
        entries.try_reserve_exact(capacity)?;
        Ok(Self { capacity, entries })
    }

    pub(crate) fn try_reserve(
        &mut self,
        group_id: GroupId,
    ) -> Result<ClassicCoordinatorInvalidationPermit<'_>, ClassicCoordinatorInvalidationReserveError>
    {
        if self.blocks_join(group_id) {
            return Err(ClassicCoordinatorInvalidationReserveError::Duplicate { group_id });
        }
        if self.retained_count() >= self.capacity {
            return Err(ClassicCoordinatorInvalidationReserveError::Capacity {
                limit: self.capacity,
            });
        }
        Ok(ClassicCoordinatorInvalidationPermit {
            group_id,
            entries: &mut self.entries,
        })
    }

    pub(crate) fn blocks_join(&self, group_id: GroupId) -> bool {
        self.entries
            .iter()
            .any(|entry| entry.group_id() == group_id)
    }

    pub(crate) fn retained_count(&self) -> usize {
        self.entries.len()
    }
}

/// Why bounded invalidation ownership could not be preflighted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ClassicCoordinatorInvalidationReserveError {
    Capacity { limit: usize },
    Duplicate { group_id: GroupId },
}

/// Failed installation retaining the exact transferred capability.
#[must_use = "failed invalidation installation still owns its coordinator capability"]
pub(crate) struct ClassicCoordinatorInvalidationInstallFailure {
    expected: GroupId,
    pending: PendingClassicCoordinatorInvalidation,
}

impl ClassicCoordinatorInvalidationInstallFailure {
    pub(crate) const fn expected_group_id(&self) -> GroupId {
        self.expected
    }

    pub(crate) const fn pending_group_id(&self) -> GroupId {
        self.pending.group_id()
    }

    pub(crate) fn discard_after_driver_shutdown(self) -> GroupId {
        let group_id = self.pending.group_id();
        drop(self.pending);
        group_id
    }
}

/// A terminal driver disposition that permits the core-planned rejoin.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ClassicCoordinatorInvalidationPermission {
    Applied,
    IgnoredStale,
}

/// A terminal invalidation outcome that cannot permit a fresh Join.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ClassicCoordinatorInvalidationTerminalFailure {
    Unavailable,
    CapacityReached,
    Completion(CompletionError),
    UnrecognizedDisposition,
}

/// One group-tagged terminal from invalidation execution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ClassicCoordinatorInvalidationTerminal {
    group_id: GroupId,
    result: Result<
        ClassicCoordinatorInvalidationPermission,
        ClassicCoordinatorInvalidationTerminalFailure,
    >,
}

impl ClassicCoordinatorInvalidationTerminal {
    pub(super) const fn new(
        group_id: GroupId,
        result: Result<
            ClassicCoordinatorInvalidationPermission,
            ClassicCoordinatorInvalidationTerminalFailure,
        >,
    ) -> Self {
        Self { group_id, result }
    }

    pub(crate) const fn group_id(self) -> GroupId {
        self.group_id
    }

    pub(crate) const fn result(
        self,
    ) -> Result<
        ClassicCoordinatorInvalidationPermission,
        ClassicCoordinatorInvalidationTerminalFailure,
    > {
        self.result
    }
}

/// One fairness-bounded invalidation drive observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ClassicCoordinatorInvalidationPoll {
    Idle,
    Submitted { group_id: GroupId },
    Pending { group_id: GroupId },
    Terminal(ClassicCoordinatorInvalidationTerminal),
}
