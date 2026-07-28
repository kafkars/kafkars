//! Inert public configuration for one bounded classic-group registration.

use std::{sync::Arc, time::Duration};

use kafka_client_core::ClassicProcessingLeasePolicy;

use crate::config::ConsumerReadIsolation;

const DEFAULT_PROCESSING_TIMEOUT: Duration = Duration::from_secs(300);

type ValidatedGroupConsumerRegistration = (
    Arc<str>,
    Option<Arc<str>>,
    Vec<Arc<str>>,
    ConsumerReadIsolation,
    ClassicProcessingLeasePolicy,
);

/// Exact caller-owned names for one bounded classic-group registration.
#[derive(Debug)]
pub struct GroupConsumerRegistration {
    group: Arc<str>,
    group_instance_id: Option<Arc<str>>,
    topics: Vec<Arc<str>>,
    read_isolation: ConsumerReadIsolation,
    processing_timeout: Duration,
}

impl GroupConsumerRegistration {
    /// Creates an inert registration request without starting membership work.
    pub fn new(group: Arc<str>, topics: Vec<Arc<str>>) -> Self {
        Self {
            group,
            group_instance_id: None,
            topics,
            read_isolation: ConsumerReadIsolation::ReadUncommitted,
            processing_timeout: DEFAULT_PROCESSING_TIMEOUT,
        }
    }

    /// Selects one stable classic-group member identity before registration.
    pub fn with_group_instance_id(mut self, group_instance_id: Arc<str>) -> Self {
        self.group_instance_id = Some(group_instance_id);
        self
    }

    /// Returns the requested static member identity, when configured.
    pub fn group_instance_id(&self) -> Option<&str> {
        self.group_instance_id.as_deref()
    }

    /// Returns the requested Kafka group spelling.
    pub fn group(&self) -> &str {
        &self.group
    }

    /// Returns the requested local topic subscription in caller order.
    pub fn topics(&self) -> &[Arc<str>] {
        &self.topics
    }

    /// Selects immutable application-record visibility before registration.
    ///
    /// The default is [`ConsumerReadIsolation::ReadUncommitted`].
    pub const fn with_read_isolation(mut self, read_isolation: ConsumerReadIsolation) -> Self {
        self.read_isolation = read_isolation;
        self
    }

    /// Returns the immutable application-record visibility for this group.
    pub const fn read_isolation(&self) -> ConsumerReadIsolation {
        self.read_isolation
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
        Ok((
            self.group,
            self.group_instance_id,
            self.topics,
            self.read_isolation,
            processing_policy,
        ))
    }
}
