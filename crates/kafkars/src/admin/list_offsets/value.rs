//! Stable successful Admin `ListOffsets` facts.

/// Kafka's successful offset-selection result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ListOffsetsResultInfo {
    offset: Option<i64>,
    timestamp_ms: Option<i64>,
    leader_epoch: Option<i32>,
}

impl ListOffsetsResultInfo {
    pub(crate) const fn new(
        offset: Option<i64>,
        timestamp_ms: Option<i64>,
        leader_epoch: Option<i32>,
    ) -> Self {
        Self {
            offset,
            timestamp_ms,
            leader_epoch,
        }
    }

    /// Returns Kafka's selected nonnegative offset, if one exists.
    pub const fn offset(&self) -> Option<i64> {
        self.offset
    }

    /// Returns Kafka's associated nonnegative timestamp, if represented.
    pub const fn timestamp_ms(&self) -> Option<i64> {
        self.timestamp_ms
    }

    /// Returns Kafka's selected nonnegative leader epoch, if represented.
    pub const fn leader_epoch(&self) -> Option<i32> {
        self.leader_epoch
    }
}
