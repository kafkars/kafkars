//! Stable failures for notification-backed event observation.

/// Stable reason an assigned-consumer event operation terminated.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AssignedConsumerNextEventErrorKind {
    /// The synchronized engine host can no longer expose retained events.
    HostUnavailable,
    /// A non-semantic observation mechanism violated its ownership contract.
    InternalInvariant,
}

/// Failure while waiting for one retained assigned-consumer event.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AssignedConsumerNextEventError {
    kind: AssignedConsumerNextEventErrorKind,
}

impl AssignedConsumerNextEventError {
    pub(super) const fn host_unavailable() -> Self {
        Self {
            kind: AssignedConsumerNextEventErrorKind::HostUnavailable,
        }
    }

    pub(super) const fn internal_invariant() -> Self {
        Self {
            kind: AssignedConsumerNextEventErrorKind::InternalInvariant,
        }
    }

    /// Returns the stable event-observation failure category.
    pub const fn kind(&self) -> AssignedConsumerNextEventErrorKind {
        self.kind
    }
}

impl std::fmt::Display for AssignedConsumerNextEventError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "assigned-consumer event wait failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for AssignedConsumerNextEventError {}
