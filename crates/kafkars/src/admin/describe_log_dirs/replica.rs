//! Stable scalar facts for one replica log in a broker log directory.

/// One current or future replica log without generated wire or engine values.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogDirReplica {
    topic_name: String,
    partition_index: i32,
    partition_size: i64,
    offset_lag: i64,
    is_future: bool,
}

impl LogDirReplica {
    pub(crate) const fn new(
        topic_name: String,
        partition_index: i32,
        partition_size: i64,
        offset_lag: i64,
        is_future: bool,
    ) -> Self {
        Self {
            topic_name,
            partition_index,
            partition_size,
            offset_lag,
            is_future,
        }
    }

    /// Returns the replica's topic name.
    pub fn topic_name(&self) -> &str {
        &self.topic_name
    }

    /// Returns the exact signed partition index reported by Kafka.
    pub const fn partition_index(&self) -> i32 {
        self.partition_index
    }

    /// Returns the exact signed log-segment size in bytes reported by Kafka.
    pub const fn partition_size(&self) -> i64 {
        self.partition_size
    }

    /// Returns the exact signed offset lag reported by Kafka.
    pub const fn offset_lag(&self) -> i64 {
        self.offset_lag
    }

    /// Returns whether this is the future replacement replica log.
    pub const fn is_future(&self) -> bool {
        self.is_future
    }
}
