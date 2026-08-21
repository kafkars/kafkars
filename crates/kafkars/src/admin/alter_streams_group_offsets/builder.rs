//! Inert `StreamsGroup` offset alteration with one delegated submission boundary.

use std::time::Duration;

use super::AlterStreamsGroupOffsets;
use crate::AlterConsumerGroupOffsetsBuilder;

/// Typed `StreamsGroup` view of the existing caller-ordered offset alteration.
///
/// Kafka 4.2 defines `StreamsGroup` offset alteration with the same offset data
/// and execution semantics as consumer-group offset alteration. This wrapper
/// preserves that single implementation and its original deadline boundary.
#[must_use = "call submit to admit the AlterStreamsGroupOffsets operation"]
pub struct AlterStreamsGroupOffsetsBuilder {
    inner: AlterConsumerGroupOffsetsBuilder,
}

impl AlterStreamsGroupOffsetsBuilder {
    pub(crate) const fn from_consumer_group(inner: AlterConsumerGroupOffsetsBuilder) -> Self {
        Self { inner }
    }

    /// Replaces the duration converted into an absolute deadline at submission.
    pub fn deadline_after(self, timeout: Duration) -> Self {
        Self {
            inner: self.inner.deadline_after(timeout),
        }
    }

    /// Delegates to the consumer-group submission boundary and returns a typed observer.
    ///
    /// This call remains the only public operation boundary. The underlying
    /// builder captures its absolute deadline before validation or admission.
    pub fn submit(self) -> AlterStreamsGroupOffsets {
        AlterStreamsGroupOffsets::from_consumer_group(self.inner.submit())
    }
}

impl std::fmt::Debug for AlterStreamsGroupOffsetsBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AlterStreamsGroupOffsetsBuilder")
            .field("consumer_group_operation", &self.inner)
            .finish()
    }
}
