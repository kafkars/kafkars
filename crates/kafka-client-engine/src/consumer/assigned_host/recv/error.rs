//! Stable failures for notification-backed batch observation.

/// Stable reason an assigned-consumer receive operation terminated.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AssignedConsumerRecvErrorKind {
    /// The synchronized engine host can no longer expose retained batches.
    HostUnavailable,
    /// A non-semantic observation mechanism violated its ownership contract.
    InternalInvariant,
}

/// Failure while waiting for one already-authorized batch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AssignedConsumerRecvError {
    kind: AssignedConsumerRecvErrorKind,
}

impl AssignedConsumerRecvError {
    pub(super) const fn host_unavailable() -> Self {
        Self {
            kind: AssignedConsumerRecvErrorKind::HostUnavailable,
        }
    }

    pub(super) const fn internal_invariant() -> Self {
        Self {
            kind: AssignedConsumerRecvErrorKind::InternalInvariant,
        }
    }

    /// Returns the stable receive-failure category.
    pub const fn kind(&self) -> AssignedConsumerRecvErrorKind {
        self.kind
    }
}

impl std::fmt::Display for AssignedConsumerRecvError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "assigned-consumer receive failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for AssignedConsumerRecvError {}
