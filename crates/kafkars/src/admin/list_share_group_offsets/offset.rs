//! Stable public value for one successful `ShareGroup` partition offset.

/// One `ShareGroup` partition's broker-visible offset state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShareGroupOffset {
    topic_id: [u8; 16],
    start_offset: Option<i64>,
    leader_epoch: Option<i32>,
    lag: Option<i64>,
}

impl ShareGroupOffset {
    pub(crate) const fn new(
        topic_id: [u8; 16],
        start_offset: Option<i64>,
        leader_epoch: Option<i32>,
        lag: Option<i64>,
    ) -> Self {
        Self {
            topic_id,
            start_offset,
            leader_epoch,
            lag,
        }
    }

    /// Returns Kafka's nonzero topic identity.
    pub const fn topic_id(&self) -> [u8; 16] {
        self.topic_id
    }

    /// Returns the share-partition start offset when Kafka supplied one.
    pub const fn start_offset(&self) -> Option<i64> {
        self.start_offset
    }

    /// Returns the leader epoch when Kafka supplied one.
    pub const fn leader_epoch(&self) -> Option<i32> {
        self.leader_epoch
    }

    /// Returns the share-partition lag when the negotiated version supplies it.
    pub const fn lag(&self) -> Option<i64> {
        self.lag
    }
}
