//! Stable path and lag facts for one current or future replica placement.

/// One current or future broker log-directory placement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaLogDirLocation {
    path: String,
    offset_lag: i64,
}

impl ReplicaLogDirLocation {
    pub(crate) const fn new(path: String, offset_lag: i64) -> Self {
        Self { path, offset_lag }
    }

    /// Returns the exact broker log-directory path.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Returns Kafka's exact signed offset lag.
    pub const fn offset_lag(&self) -> i64 {
        self.offset_lag
    }
}
