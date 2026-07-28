//! Stable failure categories for classic-group event observation.

/// Stable reason current confirmed membership could not be observed immediately.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupConsumerStateErrorKind {
    /// Another owner currently holds the classic-group shard.
    Contended,
    /// The synchronized engine host can no longer expose group state.
    HostUnavailable,
    /// Snapshot storage could not be reserved within the bounded assignment.
    Allocation,
    /// Internal group ownership was inconsistent.
    InternalInvariant,
}

/// Failure to observe current confirmed membership without waiting.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GroupConsumerStateError {
    kind: GroupConsumerStateErrorKind,
}

impl GroupConsumerStateError {
    pub(in crate::consumer) const fn new(kind: GroupConsumerStateErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable immediate-observation category.
    pub const fn kind(self) -> GroupConsumerStateErrorKind {
        self.kind
    }
}

impl std::fmt::Display for GroupConsumerStateError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "classic-group state observation failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for GroupConsumerStateError {}
