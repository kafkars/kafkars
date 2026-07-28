//! Stable volume and replica facts for one successful broker log directory.

use super::LogDirReplica;

/// One successful log-directory description.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogDirDescription {
    total_bytes: Option<i64>,
    usable_bytes: Option<i64>,
    is_cordoned: Option<bool>,
    replicas: Vec<LogDirReplica>,
}

impl LogDirDescription {
    pub(crate) const fn new(
        total_bytes: Option<i64>,
        usable_bytes: Option<i64>,
        is_cordoned: Option<bool>,
        replicas: Vec<LogDirReplica>,
    ) -> Self {
        Self {
            total_bytes,
            usable_bytes,
            is_cordoned,
            replicas,
        }
    }

    /// Returns total local volume bytes when represented by the negotiated version.
    pub const fn total_bytes(&self) -> Option<i64> {
        self.total_bytes
    }

    /// Returns usable local volume bytes when represented by the negotiated version.
    pub const fn usable_bytes(&self) -> Option<i64> {
        self.usable_bytes
    }

    /// Returns cordon state when represented by the negotiated version.
    pub const fn is_cordoned(&self) -> Option<bool> {
        self.is_cordoned
    }

    /// Returns replica logs in deterministic topic, partition, and future-log order.
    pub fn replicas(&self) -> &[LogDirReplica] {
        &self.replicas
    }

    /// Consumes this description into its replica logs.
    pub fn into_replicas(self) -> Vec<LogDirReplica> {
        self.replicas
    }
}
