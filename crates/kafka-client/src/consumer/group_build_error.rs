//! Exact facade builder ownership returned by rejected group registration.

use core::fmt;

use crate::KafkaError;

use super::ConsumerBuilder;

/// Failure to reserve one bounded classic-group registration.
///
/// The exact consumed builder remains available because no group handle
/// transferred to the caller.
#[derive(Debug)]
pub struct ConsumerBuildError {
    builder: ConsumerBuilder,
    error: KafkaError,
}

impl ConsumerBuildError {
    pub(crate) const fn new(builder: ConsumerBuilder, error: KafkaError) -> Self {
        Self { builder, error }
    }

    /// Borrows the builder whose registration was rejected.
    pub const fn builder(&self) -> &ConsumerBuilder {
        &self.builder
    }

    /// Borrows the stable semantic rejection.
    pub const fn error(&self) -> &KafkaError {
        &self.error
    }

    /// Returns the exact builder and its registration error.
    pub fn into_parts(self) -> (ConsumerBuilder, KafkaError) {
        (self.builder, self.error)
    }
}

impl fmt::Display for ConsumerBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(formatter)
    }
}

impl std::error::Error for ConsumerBuildError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}
