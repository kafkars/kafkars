//! Stable failures for notification-backed classic-group batch observation.

use crate::consumer::GroupConsumerPositionFailureKind;

/// Stable reason a named group receive cannot continue.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupConsumerRecvErrorKind {
    /// The synchronized group host is unavailable or retained a terminal fault.
    HostUnavailable,
    /// Group-position bootstrap or reset reached one exact terminal outcome.
    Position(GroupConsumerPositionFailureKind),
    /// An internal observation mechanism violated its ownership contract.
    InternalInvariant,
}

/// Named receive failure after no group batch transferred.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GroupConsumerRecvError {
    kind: GroupConsumerRecvErrorKind,
}

impl GroupConsumerRecvError {
    pub(super) const fn host_unavailable() -> Self {
        Self {
            kind: GroupConsumerRecvErrorKind::HostUnavailable,
        }
    }

    pub(super) const fn position(kind: GroupConsumerPositionFailureKind) -> Self {
        Self {
            kind: GroupConsumerRecvErrorKind::Position(kind),
        }
    }

    pub(in crate::consumer) const fn internal_invariant() -> Self {
        Self {
            kind: GroupConsumerRecvErrorKind::InternalInvariant,
        }
    }

    /// Returns the stable receive-failure category.
    pub const fn kind(&self) -> GroupConsumerRecvErrorKind {
        self.kind
    }
}

impl std::fmt::Display for GroupConsumerRecvError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "classic-group receive observation failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for GroupConsumerRecvError {}
