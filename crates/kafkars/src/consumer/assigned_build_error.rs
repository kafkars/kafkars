//! Exact facade builder ownership returned by a rejected one-shot claim.

use core::fmt;

use crate::KafkaError;

use super::AssignedConsumerBuilder;

/// Failure to claim one client's directly assigned consumer.
///
/// The exact consumed builder remains available because the engine did not
/// transfer its unique assigned-consumer capability to this call.
#[derive(Debug)]
pub struct AssignedConsumerBuildError {
    builder: AssignedConsumerBuilder,
    error: KafkaError,
}

impl AssignedConsumerBuildError {
    pub(crate) const fn new(builder: AssignedConsumerBuilder, error: KafkaError) -> Self {
        Self { builder, error }
    }

    /// Borrows the builder whose claim was rejected.
    pub const fn builder(&self) -> &AssignedConsumerBuilder {
        &self.builder
    }

    /// Borrows the stable semantic reason the claim was rejected.
    pub const fn error(&self) -> &KafkaError {
        &self.error
    }

    /// Returns the exact builder and its claim error.
    pub fn into_parts(self) -> (AssignedConsumerBuilder, KafkaError) {
        (self.builder, self.error)
    }
}

impl fmt::Display for AssignedConsumerBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(formatter)
    }
}

impl std::error::Error for AssignedConsumerBuildError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}
