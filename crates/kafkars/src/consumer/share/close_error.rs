//! Exact public share ownership returned by rejected close admission.

use core::fmt;

use crate::KafkaError;

use super::ShareConsumer;

/// Pre-admission share close rejection retaining the exact consumer.
#[must_use = "close rejection retains the exact share consumer for retry"]
pub struct ShareConsumerCloseAdmissionError {
    consumer: ShareConsumer,
    error: KafkaError,
}

impl ShareConsumerCloseAdmissionError {
    pub(crate) const fn new(consumer: ShareConsumer, error: KafkaError) -> Self {
        Self { consumer, error }
    }

    /// Borrows the stable semantic rejection.
    pub const fn error(&self) -> &KafkaError {
        &self.error
    }

    /// Borrows the exact share consumer whose close did not begin.
    pub const fn consumer(&self) -> &ShareConsumer {
        &self.consumer
    }

    /// Returns the exact share consumer and stable semantic rejection.
    pub fn into_parts(self) -> (ShareConsumer, KafkaError) {
        (self.consumer, self.error)
    }
}

impl fmt::Debug for ShareConsumerCloseAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ShareConsumerCloseAdmissionError")
            .field("consumer", &self.consumer)
            .field("error", &self.error)
            .finish()
    }
}

impl fmt::Display for ShareConsumerCloseAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(formatter)
    }
}

impl std::error::Error for ShareConsumerCloseAdmissionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}
