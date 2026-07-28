//! Closed failure categories for group-registry host turns and shutdown.

use super::{
    classic_group_execution::ClassicGroupExecutionError,
    classic_group_graceful_revocation::ClassicGroupRevocationHostError,
    offset_commit::GroupOffsetCommitHostError, registry_close::GroupConsumerRemovalError,
    registry_fetch::GroupConsumerFetchError, registry_processing::GroupConsumerProcessingError,
};

/// Concrete private group-host failure without widening operation internals.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct GroupConsumerHostError {
    kind: GroupConsumerHostErrorKind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GroupConsumerHostErrorKind {
    OffsetCommit(GroupOffsetCommitHostError),
    Membership(ClassicGroupExecutionError),
    Fetch(GroupConsumerFetchError),
    Processing(GroupConsumerProcessingError),
    GracefulRevocation(ClassicGroupRevocationHostError),
    Close(GroupConsumerRemovalError),
    MembershipUnsettled(usize),
    FetchUnsettled(usize),
    ProcessingUnsettled(usize),
    GracefulRevocationUnsettled(usize),
}

impl GroupConsumerHostError {
    pub(super) const fn membership(error: ClassicGroupExecutionError) -> Self {
        Self {
            kind: GroupConsumerHostErrorKind::Membership(error),
        }
    }

    pub(super) const fn membership_unsettled(count: usize) -> Self {
        Self {
            kind: GroupConsumerHostErrorKind::MembershipUnsettled(count),
        }
    }

    pub(super) const fn fetch(error: GroupConsumerFetchError) -> Self {
        Self {
            kind: GroupConsumerHostErrorKind::Fetch(error),
        }
    }

    pub(super) const fn fetch_unsettled(count: usize) -> Self {
        Self {
            kind: GroupConsumerHostErrorKind::FetchUnsettled(count),
        }
    }

    pub(super) const fn processing(error: GroupConsumerProcessingError) -> Self {
        Self {
            kind: GroupConsumerHostErrorKind::Processing(error),
        }
    }

    pub(super) const fn processing_unsettled(count: usize) -> Self {
        Self {
            kind: GroupConsumerHostErrorKind::ProcessingUnsettled(count),
        }
    }

    pub(super) const fn graceful_revocation(error: ClassicGroupRevocationHostError) -> Self {
        Self {
            kind: GroupConsumerHostErrorKind::GracefulRevocation(error),
        }
    }

    pub(super) const fn graceful_revocation_unsettled(count: usize) -> Self {
        Self {
            kind: GroupConsumerHostErrorKind::GracefulRevocationUnsettled(count),
        }
    }

    pub(super) const fn close(error: GroupConsumerRemovalError) -> Self {
        Self {
            kind: GroupConsumerHostErrorKind::Close(error),
        }
    }
}

impl core::fmt::Display for GroupConsumerHostError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match &self.kind {
            GroupConsumerHostErrorKind::OffsetCommit(error) => error.fmt(formatter),
            GroupConsumerHostErrorKind::Membership(error) => {
                write!(formatter, "classic membership execution failed: {error:?}")
            }
            GroupConsumerHostErrorKind::Fetch(error) => {
                write!(formatter, "classic group Fetch execution failed: {error:?}")
            }
            GroupConsumerHostErrorKind::Processing(error) => write!(
                formatter,
                "classic group processing-liveness execution failed: {error:?}"
            ),
            GroupConsumerHostErrorKind::GracefulRevocation(error) => write!(
                formatter,
                "classic group graceful revocation failed: {error:?}"
            ),
            GroupConsumerHostErrorKind::Close(error) => {
                write!(formatter, "classic group close removal failed: {error:?}")
            }
            GroupConsumerHostErrorKind::MembershipUnsettled(count) => {
                write!(
                    formatter,
                    "{count} classic membership obligations remain unsettled"
                )
            }
            GroupConsumerHostErrorKind::FetchUnsettled(count) => {
                write!(
                    formatter,
                    "{count} classic group Fetch obligations remain unsettled"
                )
            }
            GroupConsumerHostErrorKind::ProcessingUnsettled(count) => write!(
                formatter,
                "{count} classic group processing-liveness obligations remain unsettled"
            ),
            GroupConsumerHostErrorKind::GracefulRevocationUnsettled(count) => write!(
                formatter,
                "{count} classic group graceful-revocation obligations remain unsettled"
            ),
        }
    }
}

impl std::error::Error for GroupConsumerHostError {}

impl From<GroupOffsetCommitHostError> for GroupConsumerHostError {
    fn from(error: GroupOffsetCommitHostError) -> Self {
        Self {
            kind: GroupConsumerHostErrorKind::OffsetCommit(error),
        }
    }
}
