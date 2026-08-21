//! Stable public offset-selection policy for Admin `ListOffsets`.

/// One Kafka offset-selection policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OffsetSpec {
    /// Select the earliest available offset.
    Earliest,
    /// Select the latest available offset.
    Latest,
    /// Select the record offset carrying the greatest timestamp.
    MaxTimestamp,
    /// Select the earliest offset guaranteed to remain in the leader's local log.
    EarliestLocal,
    /// Select the greatest offset already retained in tiered storage.
    LatestTiered,
    /// Select the earliest offset not yet uploaded to tiered storage.
    EarliestPendingUpload,
    /// Select the earliest offset whose record timestamp is at least this value.
    Timestamp(i64),
}

impl OffsetSpec {
    /// Selects the earliest available offset.
    pub const fn earliest() -> Self {
        Self::Earliest
    }

    /// Selects the latest available offset.
    pub const fn latest() -> Self {
        Self::Latest
    }

    /// Selects the record offset carrying the greatest timestamp.
    pub const fn max_timestamp() -> Self {
        Self::MaxTimestamp
    }

    /// Selects the local-log start offset.
    pub const fn earliest_local() -> Self {
        Self::EarliestLocal
    }

    /// Selects the greatest offset already retained in tiered storage.
    pub const fn latest_tiered() -> Self {
        Self::LatestTiered
    }

    /// Selects the earliest offset not yet uploaded to tiered storage.
    pub const fn earliest_pending_upload() -> Self {
        Self::EarliestPendingUpload
    }

    /// Selects by caller-supplied Unix epoch timestamp in milliseconds.
    ///
    /// Negative timestamps remain inert until `submit()` validates the request.
    pub const fn for_timestamp(timestamp_ms: i64) -> Self {
        Self::Timestamp(timestamp_ms)
    }
}
