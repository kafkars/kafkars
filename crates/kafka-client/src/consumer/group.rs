//! Bounded classic-group registration without premature delivery exposure.

use std::time::Duration;

use crate::bridge::ClientEngine;

use super::{Consumer, ConsumerBuildError};

const DEFAULT_MEMBERSHIP_START_TIMEOUT: Duration = Duration::from_secs(30);

/// Builder for one bounded group-consumer registration.
#[derive(Debug, Clone)]
pub struct ConsumerBuilder {
    engine: ClientEngine,
    group_id: String,
    topics: Vec<String>,
    processing_timeout: Duration,
}

impl ConsumerBuilder {
    pub(crate) fn new(engine: ClientEngine, group_id: String) -> Self {
        Self {
            engine,
            group_id,
            topics: Vec::new(),
            processing_timeout: Duration::from_secs(300),
        }
    }

    /// Replaces the topic subscription retained by this registration.
    pub fn subscribe<I, S>(mut self, topics: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.topics = topics.into_iter().map(Into::into).collect();
        self
    }

    /// Selects the maximum interval between application progress observations;
    /// defaults to 300 seconds independently of session and heartbeat timing.
    pub const fn processing_timeout(mut self, processing_timeout: Duration) -> Self {
        self.processing_timeout = processing_timeout;
        self
    }

    /// Returns the requested Kafka group spelling.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Returns the caller-ordered requested subscription.
    pub fn subscription(&self) -> &[String] {
        &self.topics
    }

    /// Returns the requested application-processing timeout.
    pub const fn selected_processing_timeout(&self) -> Duration {
        self.processing_timeout
    }

    /// Registers this group and begins real hosted membership.
    ///
    /// The membership deadline is captured at this call boundary before
    /// validation or name conversion. A true pre-core rejection releases the
    /// fresh registration and returns this exact builder.
    pub fn build(self) -> Result<Consumer, ConsumerBuildError> {
        let capture = match self
            .engine
            .capture_group_consumer_start(DEFAULT_MEMBERSHIP_START_TIMEOUT)
        {
            Ok(capture) => capture,
            Err(error) => return Err(ConsumerBuildError::new(self, error)),
        };
        let engine = match self.engine.register_group_consumer(
            capture,
            &self.group_id,
            &self.topics,
            self.processing_timeout,
        ) {
            Ok(engine) => engine,
            Err(error) => return Err(ConsumerBuildError::new(self, error)),
        };
        Ok(Consumer {
            engine,
            group_id: self.group_id,
            topics: self.topics,
        })
    }
}
