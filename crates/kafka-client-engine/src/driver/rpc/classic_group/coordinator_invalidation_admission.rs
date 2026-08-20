//! Driver-neutral observation of ownership-preserving invalidation rejection.

use kafka_client_core::GroupId;
use kafka_driver::SubmitError;

/// Rejected driver admission while the exact token remains queued.
#[must_use = "invalidation admission failure leaves the exact token queued for policy"]
pub(crate) struct ClassicCoordinatorInvalidationAdmissionFailure {
    group_id: GroupId,
    source: SubmitError,
}

impl ClassicCoordinatorInvalidationAdmissionFailure {
    pub(super) const fn new(group_id: GroupId, source: SubmitError) -> Self {
        Self { group_id, source }
    }

    pub(crate) const fn group_id(&self) -> GroupId {
        self.group_id
    }

    #[allow(
        unreachable_patterns,
        reason = "the published driver RC exposes a non-exhaustive admission error while the reviewed path dependency is exhaustive"
    )]
    pub(crate) const fn kind(&self) -> ClassicCoordinatorInvalidationAdmissionFailureKind {
        match &self.source {
            SubmitError::Full => ClassicCoordinatorInvalidationAdmissionFailureKind::Full,
            SubmitError::Closed => ClassicCoordinatorInvalidationAdmissionFailureKind::Closed,
            SubmitError::Wake(_) => ClassicCoordinatorInvalidationAdmissionFailureKind::Wake,
            SubmitError::IdentityExhausted => {
                ClassicCoordinatorInvalidationAdmissionFailureKind::IdentityExhausted
            }
            SubmitError::ForeignDriver => {
                ClassicCoordinatorInvalidationAdmissionFailureKind::ForeignDriver
            }
            SubmitError::VersionBoundsInvalid { .. } => {
                ClassicCoordinatorInvalidationAdmissionFailureKind::VersionBoundsInvalid
            }
            _ => ClassicCoordinatorInvalidationAdmissionFailureKind::Unrecognized,
        }
    }
}

/// Driver-neutral category for one ownership-preserving admission rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ClassicCoordinatorInvalidationAdmissionFailureKind {
    Full,
    Closed,
    Wake,
    IdentityExhausted,
    ForeignDriver,
    VersionBoundsInvalid,
    Unrecognized,
}
