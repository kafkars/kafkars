//! Immutable correlation and offset-window facts for one successful Fetch.

/// Broker-correlated facts retained beside one exact normalized Fetch output.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FetchSuccessEvidence {
    topic_uuid: Option<[u8; 16]>,
    requested_offset: i64,
    next_offset: i64,
    log_start_offset: Option<i64>,
    last_stable_offset: Option<i64>,
    high_watermark: Option<i64>,
}

impl FetchSuccessEvidence {
    pub(super) const fn new(
        topic_uuid: Option<[u8; 16]>,
        requested_offset: i64,
        next_offset: i64,
        log_start_offset: Option<i64>,
        last_stable_offset: Option<i64>,
        high_watermark: Option<i64>,
    ) -> Self {
        Self {
            topic_uuid,
            requested_offset,
            next_offset,
            log_start_offset,
            last_stable_offset,
            high_watermark,
        }
    }

    pub(crate) const fn topic_uuid(self) -> Option<[u8; 16]> {
        self.topic_uuid
    }

    pub(crate) const fn requested_offset(self) -> i64 {
        self.requested_offset
    }

    pub(crate) const fn next_offset(self) -> i64 {
        self.next_offset
    }

    pub(crate) const fn log_start_offset(self) -> Option<i64> {
        self.log_start_offset
    }

    pub(crate) const fn last_stable_offset(self) -> Option<i64> {
        self.last_stable_offset
    }

    pub(crate) const fn high_watermark(self) -> Option<i64> {
        self.high_watermark
    }

    pub(crate) const fn advanced(self) -> bool {
        self.next_offset > self.requested_offset
    }
}
