//! Stable failures for notification-backed share batch observation.

/// Stable reason a named share receive cannot continue.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShareConsumerRecvErrorKind {
    /// The synchronized share host is unavailable or retained a terminal fault.
    HostUnavailable,
    /// An internal observation mechanism violated its ownership contract.
    InternalInvariant,
}

/// Named receive failure after no share batch transferred.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShareConsumerRecvError {
    kind: ShareConsumerRecvErrorKind,
}

impl ShareConsumerRecvError {
    pub(super) const fn host_unavailable() -> Self {
        Self {
            kind: ShareConsumerRecvErrorKind::HostUnavailable,
        }
    }

    pub(in crate::consumer) const fn internal_invariant() -> Self {
        Self {
            kind: ShareConsumerRecvErrorKind::InternalInvariant,
        }
    }

    /// Returns the stable receive-failure category.
    pub const fn kind(self) -> ShareConsumerRecvErrorKind {
        self.kind
    }
}

impl std::fmt::Display for ShareConsumerRecvError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "share-consumer receive observation failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for ShareConsumerRecvError {}
