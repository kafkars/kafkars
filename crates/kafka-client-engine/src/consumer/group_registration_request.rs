//! Inert public configuration for one bounded classic-group registration.

use std::{sync::Arc, time::Duration};

use kafka_client_core::ClassicProcessingLeasePolicy;

const DEFAULT_PROCESSING_TIMEOUT: Duration = Duration::from_secs(300);

type ValidatedGroupConsumerRegistration = (Arc<str>, Vec<Arc<str>>, ClassicProcessingLeasePolicy);

/// Exact caller-owned names for one bounded classic-group registration.
#[derive(Debug)]
pub struct GroupConsumerRegistration {
    group: Arc<str>,
    topics: Vec<Arc<str>>,
    processing_timeout: Duration,
}

impl GroupConsumerRegistration {
    /// Creates an inert registration request without starting membership work.
    pub fn new(group: Arc<str>, topics: Vec<Arc<str>>) -> Self {
        Self {
            group,
            topics,
            processing_timeout: DEFAULT_PROCESSING_TIMEOUT,
        }
    }

    /// Returns the requested Kafka group spelling.
    pub fn group(&self) -> &str {
        &self.group
    }

    /// Returns the requested local topic subscription in caller order.
    pub fn topics(&self) -> &[Arc<str>] {
        &self.topics
    }

    /// Selects the application-processing liveness timeout.
    ///
    /// The default is 300 seconds.
    pub const fn with_processing_timeout(mut self, processing_timeout: Duration) -> Self {
        self.processing_timeout = processing_timeout;
        self
    }

    /// Returns the application-processing liveness timeout.
    pub const fn processing_timeout(&self) -> Duration {
        self.processing_timeout
    }

    pub(super) fn into_validated_parts(self) -> Result<ValidatedGroupConsumerRegistration, Self> {
        let timeout_ticks = match u64::try_from(self.processing_timeout.as_nanos()) {
            Ok(timeout_ticks) => timeout_ticks,
            Err(_overflow) => return Err(self),
        };
        let processing_policy = match ClassicProcessingLeasePolicy::try_new(timeout_ticks) {
            Ok(policy) => policy,
            Err(_invalid) => return Err(self),
        };
        Ok((self.group, self.topics, processing_policy))
    }
}
