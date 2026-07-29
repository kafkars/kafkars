//! Optional current and future placements for one selected replica.

use super::ReplicaLogDirLocation;

/// Kafka's current and future log-directory placement for one replica.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaLogDirInfo {
    current: Option<ReplicaLogDirLocation>,
    future: Option<ReplicaLogDirLocation>,
}

impl ReplicaLogDirInfo {
    pub(crate) const fn new(
        current: Option<ReplicaLogDirLocation>,
        future: Option<ReplicaLogDirLocation>,
    ) -> Self {
        Self { current, future }
    }

    /// Returns the current placement when Kafka reported one.
    pub const fn current(&self) -> Option<&ReplicaLogDirLocation> {
        self.current.as_ref()
    }

    /// Returns the future replacement placement when Kafka reported one.
    pub const fn future(&self) -> Option<&ReplicaLogDirLocation> {
        self.future.as_ref()
    }
}
