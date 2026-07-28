//! Stable successful Admin `DeleteRecords` facts.

/// Kafka's successful record-deletion result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeleteRecordsResultInfo {
    low_watermark: i64,
}

impl DeleteRecordsResultInfo {
    pub(crate) const fn new(low_watermark: i64) -> Self {
        Self { low_watermark }
    }

    /// Returns Kafka's first offset that may still be available.
    pub const fn low_watermark(&self) -> i64 {
        self.low_watermark
    }
}
