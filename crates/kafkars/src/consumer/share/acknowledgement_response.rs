//! Public generated-free response to one share acknowledgement.

use crate::bridge::share_consumer::{
    ShareAcknowledgementPartitionOutcome as BridgePartitionOutcome,
    ShareAcknowledgementResponse as BridgeResponse,
};

/// Response to one accepted share acknowledgement.
#[derive(Debug)]
pub struct ShareAcknowledgementResponse {
    inner: BridgeResponse,
}

impl ShareAcknowledgementResponse {
    pub(super) const fn from_bridge(inner: BridgeResponse) -> Self {
        Self { inner }
    }

    /// Returns Kafka's nonnegative response throttle in milliseconds.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.inner.throttle_time_ms()
    }

    /// Iterates request-correlated partition outcomes in canonical order.
    pub fn partitions(
        &self,
    ) -> impl ExactSizeIterator<Item = ShareAcknowledgementPartitionOutcome<'_>> {
        self.inner
            .partitions()
            .map(ShareAcknowledgementPartitionOutcome::from_bridge)
    }
}

/// Borrowed result for one acknowledged topic partition.
#[derive(Clone, Copy, Debug)]
pub struct ShareAcknowledgementPartitionOutcome<'response> {
    inner: BridgePartitionOutcome<'response>,
}

impl<'response> ShareAcknowledgementPartitionOutcome<'response> {
    const fn from_bridge(inner: BridgePartitionOutcome<'response>) -> Self {
        Self { inner }
    }

    /// Returns the exact Kafka topic UUID bytes.
    pub const fn topic_id(self) -> [u8; 16] {
        self.inner.topic_id()
    }

    /// Returns the zero-based partition index.
    pub const fn partition(self) -> u32 {
        self.inner.partition()
    }

    /// Returns Kafka's exact nonzero partition error code, if any.
    pub const fn broker_code(self) -> Option<i16> {
        self.inner.broker_code()
    }

    /// Returns Kafka's bounded diagnostic bytes without UTF-8 coercion.
    pub fn error_message(self) -> Option<&'response [u8]> {
        self.inner.error_message()
    }

    /// Returns Kafka's current leader id and epoch when provided.
    pub const fn current_leader(self) -> Option<(i32, i32)> {
        self.inner.current_leader()
    }
}
