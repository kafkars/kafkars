//! Exact caller-ownership return for immediate producer admission failure.

use std::fmt;

use crate::KafkaError;

/// Immediate producer-admission failure that returns caller ownership.
#[derive(Debug)]
pub struct TrySendError<T> {
    record: T,
    error: KafkaError,
}

impl<T> TrySendError<T> {
    pub(crate) const fn new(record: T, error: KafkaError) -> Self {
        Self { record, error }
    }

    /// Borrows the record whose ownership never crossed admission.
    pub const fn record(&self) -> &T {
        &self.record
    }

    /// Borrows the semantic reason admission failed.
    pub const fn error(&self) -> &KafkaError {
        &self.error
    }

    /// Returns the record and error to the caller.
    pub fn into_parts(self) -> (T, KafkaError) {
        (self.record, self.error)
    }
}

impl<T> fmt::Display for TrySendError<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(formatter)
    }
}

impl<T: fmt::Debug> std::error::Error for TrySendError<T> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}
