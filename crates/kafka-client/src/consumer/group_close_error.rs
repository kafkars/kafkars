//! Exact public consumer ownership returned by rejected close admission.

use core::fmt;

use crate::KafkaError;

use super::Consumer;

/// Pre-admission group close rejection retaining the exact consumer.
#[must_use = "close rejection retains the exact consumer for retry"]
pub struct ConsumerCloseAdmissionError {
    consumer: Consumer,
    error: KafkaError,
}

impl ConsumerCloseAdmissionError {
    pub(crate) const fn new(consumer: Consumer, error: KafkaError) -> Self {
        Self { consumer, error }
    }

    /// Borrows the stable semantic rejection.
    pub const fn error(&self) -> &KafkaError {
        &self.error
    }

    /// Borrows the exact consumer whose close did not begin.
    pub const fn consumer(&self) -> &Consumer {
        &self.consumer
    }

    /// Returns the exact consumer and stable semantic rejection.
    pub fn into_parts(self) -> (Consumer, KafkaError) {
        (self.consumer, self.error)
    }
}

impl fmt::Debug for ConsumerCloseAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConsumerCloseAdmissionError")
            .field("consumer", &self.consumer)
            .field("error", &self.error)
            .finish()
    }
}

impl fmt::Display for ConsumerCloseAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(formatter)
    }
}

impl std::error::Error for ConsumerCloseAdmissionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}
