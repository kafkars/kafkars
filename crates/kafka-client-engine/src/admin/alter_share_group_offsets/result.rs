//! Stable ordered results for Admin `AlterShareGroupOffsets`.

/// Exact partition-level Kafka rejection with its bounded diagnostic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterShareGroupOffsetsPartitionError {
    pub(super) code: i16,
    pub(super) message: Option<String>,
    pub(super) message_truncated: bool,
}

impl AlterShareGroupOffsetsPartitionError {
    /// Returns Kafka's exact signed nonzero error code.
    pub const fn code(&self) -> i16 {
        self.code
    }

    /// Returns Kafka's optional bounded diagnostic.
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    /// Reports whether the diagnostic was truncated at the retained-byte bound.
    pub const fn message_truncated(&self) -> bool {
        self.message_truncated
    }

    /// Consumes the rejection into exact stable parts.
    pub fn into_parts(self) -> (i16, Option<String>, bool) {
        (self.code, self.message, self.message_truncated)
    }
}

/// One caller-ordered topic-partition alteration result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterShareGroupOffsetsPartitionResult {
    pub(super) topic: String,
    pub(super) partition: i32,
    pub(super) result: Result<[u8; 16], AlterShareGroupOffsetsPartitionError>,
}

impl AlterShareGroupOffsetsPartitionResult {
    /// Returns the exact requested topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the exact requested partition index.
    pub const fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns Kafka's nonzero topic ID or the exact partition-local rejection.
    pub const fn result(&self) -> &Result<[u8; 16], AlterShareGroupOffsetsPartitionError> {
        &self.result
    }

    /// Consumes this result into stable identity and exact broker outcome.
    pub fn into_parts(
        self,
    ) -> (
        String,
        i32,
        Result<[u8; 16], AlterShareGroupOffsetsPartitionError>,
    ) {
        (self.topic, self.partition, self.result)
    }
}

/// Ordered successful response plus Kafka's throttle observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterShareGroupOffsetsBatch {
    pub(super) throttle_time_ms: u32,
    pub(super) partitions: Vec<AlterShareGroupOffsetsPartitionResult>,
}

impl AlterShareGroupOffsetsBatch {
    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns results in original caller topic-partition order.
    pub fn partitions(&self) -> &[AlterShareGroupOffsetsPartitionResult] {
        &self.partitions
    }

    /// Consumes the batch into throttle and caller-ordered results.
    pub fn into_parts(self) -> (u32, Vec<AlterShareGroupOffsetsPartitionResult>) {
        (self.throttle_time_ms, self.partitions)
    }
}
