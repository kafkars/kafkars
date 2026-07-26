//! Completion-fault and post-driver-shutdown ownership recovery.

use kafka_client_core::GroupPositionFence;
use kafka_driver::CompletionError;

use super::{
    calls::TrackedGroupPositionOffsetFetchCall, key::GroupPositionOffsetFetchKey,
    terminal::GroupPositionOffsetFetchTerminal,
};

/// Stable engine-local classification of a completion-cell observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupPositionOffsetFetchCompletionFailureKind {
    Closed,
    Consumed,
    Unknown,
}

impl GroupPositionOffsetFetchCompletionFailureKind {
    const fn from_driver(source: CompletionError) -> Self {
        match source {
            CompletionError::Closed => Self::Closed,
            CompletionError::Consumed => Self::Consumed,
            _ => Self::Unknown,
        }
    }
}

/// Copyable observation while completion-corruption ownership remains retained.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct GroupPositionOffsetFetchCompletionObservation {
    fence: GroupPositionFence,
    kind: GroupPositionOffsetFetchCompletionFailureKind,
}

impl GroupPositionOffsetFetchCompletionObservation {
    pub(crate) const fn fence(self) -> GroupPositionFence {
        self.fence
    }

    pub(crate) const fn kind(self) -> GroupPositionOffsetFetchCompletionFailureKind {
        self.kind
    }
}

/// Completion-cell failure retained until the driver owner is gone.
#[must_use = "completion failure retains one accepted position request"]
pub(super) struct GroupPositionOffsetFetchCompletionFailure {
    key: GroupPositionOffsetFetchKey,
    source: CompletionError,
}

impl GroupPositionOffsetFetchCompletionFailure {
    pub(super) const fn new(key: GroupPositionOffsetFetchKey, source: CompletionError) -> Self {
        Self { key, source }
    }

    pub(super) const fn fence(&self) -> GroupPositionFence {
        self.key.fence()
    }

    pub(super) const fn observation(&self) -> GroupPositionOffsetFetchCompletionObservation {
        GroupPositionOffsetFetchCompletionObservation {
            fence: self.key.fence(),
            kind: GroupPositionOffsetFetchCompletionFailureKind::from_driver(self.source),
        }
    }

    pub(super) fn into_recovery(self) -> GroupPositionOffsetFetchCompletionRecovery {
        GroupPositionOffsetFetchCompletionRecovery {
            key: self.key,
            source: self.source,
        }
    }
}

/// Post-driver-shutdown ownership of one corrupted completion.
#[must_use = "completion recovery retains one exact group position key"]
pub(crate) struct GroupPositionOffsetFetchCompletionRecovery {
    key: GroupPositionOffsetFetchKey,
    source: CompletionError,
}

impl GroupPositionOffsetFetchCompletionRecovery {
    pub(crate) fn into_parts(
        self,
    ) -> (
        GroupPositionOffsetFetchKey,
        GroupPositionOffsetFetchCompletionObservation,
    ) {
        let observation = GroupPositionOffsetFetchCompletionObservation {
            fence: self.key.fence(),
            kind: GroupPositionOffsetFetchCompletionFailureKind::from_driver(self.source),
        };
        (self.key, observation)
    }
}

/// Complete release of every retained RPC owner after driver destruction.
#[must_use = "shutdown recovery retains every unsettled group position owner"]
pub(crate) struct GroupPositionOffsetFetchShutdownRecovery {
    active: Vec<TrackedGroupPositionOffsetFetchCall>,
    settled: Option<GroupPositionOffsetFetchTerminal>,
    pending_fence: Option<GroupPositionFence>,
    completion: Option<GroupPositionOffsetFetchCompletionRecovery>,
}

impl GroupPositionOffsetFetchShutdownRecovery {
    pub(super) const fn new(
        active: Vec<TrackedGroupPositionOffsetFetchCall>,
        settled: Option<GroupPositionOffsetFetchTerminal>,
        pending_fence: Option<GroupPositionFence>,
        completion: Option<GroupPositionOffsetFetchCompletionRecovery>,
    ) -> Self {
        Self {
            active,
            settled,
            pending_fence,
            completion,
        }
    }

    pub(crate) fn pop_active(&mut self) -> Option<GroupPositionOffsetFetchKey> {
        self.active
            .pop()
            .map(TrackedGroupPositionOffsetFetchCall::recover_after_driver_shutdown)
    }

    pub(crate) fn take_settled(&mut self) -> Option<GroupPositionOffsetFetchTerminal> {
        self.settled.take()
    }

    pub(crate) const fn pending_fence(&self) -> Option<GroupPositionFence> {
        self.pending_fence
    }

    pub(crate) fn clear_pending_fence(&mut self) {
        self.pending_fence = None;
    }

    pub(crate) fn take_completion(&mut self) -> Option<GroupPositionOffsetFetchCompletionRecovery> {
        self.completion.take()
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.active.is_empty()
            && self.settled.is_none()
            && self.pending_fence.is_none()
            && self.completion.is_none()
    }

    pub(crate) fn retained_count(&self) -> usize {
        self.active
            .len()
            .saturating_add(usize::from(self.settled.is_some()))
            .saturating_add(usize::from(self.pending_fence.is_some()))
            .saturating_add(usize::from(self.completion.is_some()))
    }
}
