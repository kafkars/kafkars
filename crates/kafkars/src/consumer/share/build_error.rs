//! Exact facade builder ownership returned by rejected share registration.

use core::fmt;

use crate::KafkaError;

use super::ShareConsumerBuilder;

/// Failure to reserve one bounded share-member registration.
#[derive(Debug)]
pub struct ShareConsumerBuildError {
    builder: Box<ShareConsumerBuilder>,
    error: KafkaError,
}

impl ShareConsumerBuildError {
    pub(crate) fn new(builder: ShareConsumerBuilder, error: KafkaError) -> Self {
        Self {
            builder: Box::new(builder),
            error,
        }
    }

    /// Borrows the exact builder whose registration was rejected.
    pub const fn builder(&self) -> &ShareConsumerBuilder {
        &self.builder
    }

    /// Borrows the stable semantic rejection.
    pub const fn error(&self) -> &KafkaError {
        &self.error
    }

    /// Returns the exact builder and its registration error.
    pub fn into_parts(self) -> (ShareConsumerBuilder, KafkaError) {
        (*self.builder, self.error)
    }
}

impl fmt::Display for ShareConsumerBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(formatter)
    }
}

impl std::error::Error for ShareConsumerBuildError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}
