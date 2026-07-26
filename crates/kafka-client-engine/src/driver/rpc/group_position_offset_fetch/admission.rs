//! Lossless capacity, duplicate, acceptance, and driver-rejection outcomes.

use kafka_client_core::{GroupPositionBootstrapInput, GroupPositionFence};

use crate::protocol::consumer::PreparedGroupOffsetFetchRequest;

use super::{key::GroupPositionOffsetFetchKey, submission::GroupPositionOffsetFetchSubmitError};

/// Linear acknowledgment that the driver accepted one exact position request.
#[must_use = "driver acceptance must be applied to the exact core bootstrap"]
pub(crate) struct GroupPositionOffsetFetchAccepted {
    fence: GroupPositionFence,
}

impl GroupPositionOffsetFetchAccepted {
    pub(super) const fn new(fence: GroupPositionFence) -> Self {
        Self { fence }
    }

    pub(crate) const fn fence(&self) -> GroupPositionFence {
        self.fence
    }

    pub(crate) const fn driver_accepted(&self) -> GroupPositionBootstrapInput {
        GroupPositionBootstrapInput::DriverAccepted { fence: self.fence }
    }

    #[cfg(test)]
    pub(crate) const fn from_fence_for_test(fence: GroupPositionFence) -> Self {
        Self::new(fence)
    }

    pub(super) const fn confirm_receipt(self) {
        let Self { fence: _ } = self;
    }
}

/// Why the registry returned both exact pre-driver owners.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupPositionOffsetFetchReturnReason {
    Capacity { limit: usize },
    DuplicateFence,
}

/// Pre-driver ownership returned unchanged under capacity or duplicate fencing.
#[must_use = "returned group position request ownership must be retried or released"]
pub(crate) struct GroupPositionOffsetFetchReturn {
    key: GroupPositionOffsetFetchKey,
    request: PreparedGroupOffsetFetchRequest,
    reason: GroupPositionOffsetFetchReturnReason,
}

impl GroupPositionOffsetFetchReturn {
    pub(super) const fn new(
        key: GroupPositionOffsetFetchKey,
        request: PreparedGroupOffsetFetchRequest,
        reason: GroupPositionOffsetFetchReturnReason,
    ) -> Self {
        Self {
            key,
            request,
            reason,
        }
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        GroupPositionOffsetFetchKey,
        PreparedGroupOffsetFetchRequest,
        GroupPositionOffsetFetchReturnReason,
    ) {
        (self.key, self.request, self.reason)
    }
}

/// Definitely-unsent coordinator validation or driver admission rejection.
#[must_use = "driver rejection must be applied to the exact core bootstrap"]
pub(crate) struct GroupPositionOffsetFetchAdmissionFailure {
    key: GroupPositionOffsetFetchKey,
    source: GroupPositionOffsetFetchSubmitError,
}

impl GroupPositionOffsetFetchAdmissionFailure {
    pub(super) const fn new(
        key: GroupPositionOffsetFetchKey,
        source: GroupPositionOffsetFetchSubmitError,
    ) -> Self {
        Self { key, source }
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        GroupPositionOffsetFetchKey,
        GroupPositionOffsetFetchSubmitError,
    ) {
        (self.key, self.source)
    }
}

/// Result of one duplicate-checked and capacity-preflighted submission.
#[must_use = "group position admission retains exact request or acceptance ownership"]
#[expect(
    clippy::large_enum_variant,
    reason = "boxing would add an untracked allocation to lossless pre-driver owner return"
)]
pub(crate) enum GroupPositionOffsetFetchAdmission {
    Accepted(GroupPositionOffsetFetchAccepted),
    Returned(GroupPositionOffsetFetchReturn),
    Rejected(GroupPositionOffsetFetchAdmissionFailure),
}
