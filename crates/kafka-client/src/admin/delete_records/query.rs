//! Stable caller-ordered Admin `DeleteRecords` target.

const HIGH_WATERMARK_OFFSET: i64 = -1;

/// One topic-partition paired with its offset-selection policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteRecordsTarget {
    topic: String,
    partition: i32,
    before_offset: i64,
}

impl DeleteRecordsTarget {
    /// Deletes records with offsets lower than `before_offset`.
    pub fn before_offset(topic: impl Into<String>, partition: i32, before_offset: i64) -> Self {
        Self {
            topic: topic.into(),
            partition,
            before_offset,
        }
    }

    /// Deletes every currently deletable record below the high watermark.
    pub fn before_high_watermark(topic: impl Into<String>, partition: i32) -> Self {
        Self::before_offset(topic, partition, HIGH_WATERMARK_OFFSET)
    }

    /// Returns the requested topic.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the requested partition.
    pub const fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the first offset that must remain, or `-1` for high watermark.
    pub const fn deletion_offset(&self) -> i64 {
        self.before_offset
    }

    pub(crate) fn into_parts(self) -> (String, i32, i64) {
        (self.topic, self.partition, self.before_offset)
    }
}
