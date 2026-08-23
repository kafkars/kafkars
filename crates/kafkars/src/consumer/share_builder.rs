//! Capture-first construction of one unique hosted share consumer.

use std::time::Duration;

use crate::bridge::ClientEngine;

use super::{ShareConsumer, ShareConsumerBuildError, ShareConsumerFetchConfig};

const DEFAULT_MEMBERSHIP_START_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_CLOSE_TIMEOUT: Duration = Duration::from_secs(30);

/// Builder for one bounded share-group consumer registration.
#[derive(Debug, Clone)]
pub struct ShareConsumerBuilder {
    engine: ClientEngine,
    group_id: String,
    rack: Option<String>,
    topics: Vec<String>,
    fetch: ShareConsumerFetchConfig,
    membership_start_timeout: Duration,
    close_timeout: Duration,
}

impl ShareConsumerBuilder {
    pub(crate) const fn new(engine: ClientEngine, group_id: String) -> Self {
        Self {
            engine,
            group_id,
            rack: None,
            topics: Vec::new(),
            fetch: ShareConsumerFetchConfig::new(
                Duration::from_millis(500),
                1,
                1024 * 1024,
                500,
                500,
                Duration::from_secs(30),
            ),
            membership_start_timeout: DEFAULT_MEMBERSHIP_START_TIMEOUT,
            close_timeout: DEFAULT_CLOSE_TIMEOUT,
        }
    }

    /// Replaces the caller-ordered topic subscription.
    pub fn subscribe<I, S>(mut self, topics: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.topics = topics.into_iter().map(Into::into).collect();
        self
    }

    /// Sets the optional rack spelling sent by `ShareGroupHeartbeat`.
    pub fn rack(mut self, rack: impl Into<String>) -> Self {
        self.rack = Some(rack.into());
        self
    }

    /// Replaces the immutable `ShareFetch` request and attempt policy.
    #[must_use]
    pub const fn fetch_config(mut self, fetch: ShareConsumerFetchConfig) -> Self {
        self.fetch = fetch;
        self
    }

    /// Sets the end-to-end deadline for the first successful share heartbeat.
    #[must_use]
    pub const fn membership_start_timeout(mut self, timeout: Duration) -> Self {
        self.membership_start_timeout = timeout;
        self
    }

    /// Sets the end-to-end deadline for explicit graceful close.
    #[must_use]
    pub const fn close_timeout(mut self, timeout: Duration) -> Self {
        self.close_timeout = timeout;
        self
    }

    /// Returns the requested Kafka group spelling.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Returns the caller-ordered topic subscription.
    pub fn subscription(&self) -> &[String] {
        &self.topics
    }

    /// Returns the configured rack spelling.
    pub fn selected_rack(&self) -> Option<&str> {
        self.rack.as_deref()
    }

    /// Returns the configured `ShareFetch` request and attempt policy.
    pub const fn selected_fetch_config(&self) -> ShareConsumerFetchConfig {
        self.fetch
    }

    /// Returns the first-heartbeat deadline duration.
    pub const fn selected_membership_start_timeout(&self) -> Duration {
        self.membership_start_timeout
    }

    /// Returns the explicit-close deadline duration.
    pub const fn selected_close_timeout(&self) -> Duration {
        self.close_timeout
    }

    /// Registers this share member and begins bounded hosted membership.
    ///
    /// The membership deadline is captured at this call boundary before name
    /// conversion. A pre-admission rejection returns this exact builder.
    pub fn build(self) -> Result<ShareConsumer, ShareConsumerBuildError> {
        let capture = match self
            .engine
            .capture_share_consumer_start(self.membership_start_timeout)
        {
            Ok(capture) => capture,
            Err(error) => return Err(ShareConsumerBuildError::new(self, error)),
        };
        let engine = match self.engine.register_share_consumer(
            capture,
            &self.group_id,
            self.rack.as_deref(),
            &self.topics,
            self.fetch,
            self.close_timeout,
        ) {
            Ok(engine) => engine,
            Err(error) => return Err(ShareConsumerBuildError::new(self, error)),
        };
        Ok(ShareConsumer::new(
            engine,
            self.group_id,
            self.rack,
            self.topics,
        ))
    }
}
