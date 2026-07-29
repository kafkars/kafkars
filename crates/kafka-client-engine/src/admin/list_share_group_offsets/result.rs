//! Stable ordered partition results for Admin `ListShareGroupOffsets`.

/// One successful ShareGroup partition's broker-visible offset state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ListShareGroupOffsetsPartitionDescription {
    pub(super) start_offset: Option<i64>,
    pub(super) leader_epoch: Option<i32>,
    pub(super) lag: Option<i64>,
}

impl ListShareGroupOffsetsPartitionDescription {
    /// Returns the share-partition start offset when Kafka supplied one.
    pub const fn start_offset(&self) -> Option<i64> {
        self.start_offset
    }

    /// Returns the leader epoch when Kafka supplied one.
    pub const fn leader_epoch(&self) -> Option<i32> {
        self.leader_epoch
    }

    /// Returns lag when the negotiated API version supplied it.
    pub const fn lag(&self) -> Option<i64> {
        self.lag
    }

    /// Consumes the value into stable scalar parts.
    pub const fn into_parts(self) -> (Option<i64>, Option<i32>, Option<i64>) {
        (self.start_offset, self.leader_epoch, self.lag)
    }
}

/// Exact partition-level Kafka rejection with its bounded diagnostic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListShareGroupOffsetsPartitionError {
    pub(super) code: i16,
    pub(super) message: Option<String>,
    pub(super) message_truncated: bool,
}

impl ListShareGroupOffsetsPartitionError {
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

/// One selected-order or canonical-order partition result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListShareGroupOffsetsPartitionResult {
    pub(super) topic: String,
    pub(super) topic_id: [u8; 16],
    pub(super) partition: i32,
    pub(super) result:
        Result<ListShareGroupOffsetsPartitionDescription, ListShareGroupOffsetsPartitionError>,
}

impl ListShareGroupOffsetsPartitionResult {
    /// Returns the exact topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns Kafka's nonzero topic identity.
    pub const fn topic_id(&self) -> [u8; 16] {
        self.topic_id
    }

    /// Returns the nonnegative partition index.
    pub const fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the exact offset state or partition-local Kafka rejection.
    pub const fn result(
        &self,
    ) -> &Result<ListShareGroupOffsetsPartitionDescription, ListShareGroupOffsetsPartitionError>
    {
        &self.result
    }

    /// Consumes this result into stable identity and exact broker outcome.
    pub fn into_parts(
        self,
    ) -> (
        String,
        [u8; 16],
        i32,
        Result<ListShareGroupOffsetsPartitionDescription, ListShareGroupOffsetsPartitionError>,
    ) {
        (self.topic, self.topic_id, self.partition, self.result)
    }
}

/// Ordered successful response plus Kafka's throttle observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListShareGroupOffsetsBatch {
    pub(super) throttle_time_ms: u32,
    pub(super) offsets: Vec<ListShareGroupOffsetsPartitionResult>,
}

impl ListShareGroupOffsetsBatch {
    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns caller order for selected input or canonical order for all input.
    pub fn offsets(&self) -> &[ListShareGroupOffsetsPartitionResult] {
        &self.offsets
    }

    /// Consumes the batch into throttle and ordered partition results.
    pub fn into_parts(self) -> (u32, Vec<ListShareGroupOffsetsPartitionResult>) {
        (self.throttle_time_ms, self.offsets)
    }
}
