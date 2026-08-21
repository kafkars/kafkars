//! Inert group-consumer builder policy and exact selected-value views.

use std::time::Duration;

use crate::bridge::ClientEngine;

use super::{
    ClassicGroupAssignor, ClassicGroupConfig, ConsumerBuilder, ConsumerFetchConfig,
    ConsumerGroupProtocol, ConsumerLimits, DEFAULT_MEMBERSHIP_START_TIMEOUT,
    GroupConsumerOperationConfig, OffsetReset, ReadIsolation,
};

impl ConsumerBuilder {
    pub(crate) fn new(engine: ClientEngine, group_id: String) -> Self {
        Self {
            engine,
            group_id,
            group_instance_id: None,
            topics: Vec::new(),
            group_protocol: ConsumerGroupProtocol::Classic,
            classic_group_assignor: None,
            offset_reset: OffsetReset::Error,
            read_isolation: ReadIsolation::ReadUncommitted,
            processing_timeout: Duration::from_secs(300),
            membership_start_timeout: DEFAULT_MEMBERSHIP_START_TIMEOUT,
            classic_group_config: ClassicGroupConfig::default(),
            operations: GroupConsumerOperationConfig::default(),
            fetch: ConsumerFetchConfig::default(),
            limits: ConsumerLimits::default(),
        }
    }

    /// Selects one stable classic-group member identity for this registration.
    ///
    /// The identity must be a non-empty Kafka string. Omitting this option
    /// retains dynamic classic-group membership.
    pub fn group_instance_id(mut self, group_instance_id: impl Into<String>) -> Self {
        self.group_instance_id = Some(group_instance_id.into());
        self
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

    /// Selects the Kafka consumer-group membership protocol.
    ///
    /// Classic membership remains the default. Selecting
    /// [`ConsumerGroupProtocol::Consumer`] never silently falls back to
    /// classic membership.
    pub const fn group_protocol(mut self, protocol: ConsumerGroupProtocol) -> Self {
        self.group_protocol = protocol;
        self
    }

    /// Selects the classic-group partition assignor.
    ///
    /// Combining this with [`ConsumerGroupProtocol::Consumer`] is rejected.
    pub const fn classic_group_assignor(mut self, assignor: ClassicGroupAssignor) -> Self {
        self.classic_group_assignor = Some(assignor);
        self
    }

    /// Selects the explicit missing-offset policy.
    pub const fn on_missing_offset(mut self, policy: OffsetReset) -> Self {
        self.offset_reset = policy;
        self
    }

    /// Selects record visibility; defaults to [`ReadIsolation::ReadUncommitted`].
    pub const fn read_isolation(mut self, read_isolation: ReadIsolation) -> Self {
        self.read_isolation = read_isolation;
        self
    }

    /// Selects the progress interval; defaults to 300 seconds apart from membership timing.
    pub const fn processing_timeout(mut self, processing_timeout: Duration) -> Self {
        self.processing_timeout = processing_timeout;
        self
    }

    /// Selects the membership-start operation timeout captured by [`Self::build`].
    pub const fn membership_start_timeout(mut self, timeout: Duration) -> Self {
        self.membership_start_timeout = timeout;
        self
    }

    /// Sets classic session, rebalance, Heartbeat, and retry timing.
    pub const fn classic_group_config(mut self, config: ClassicGroupConfig) -> Self {
        self.classic_group_config = config;
        self
    }

    /// Sets the hosted group's seek and explicit-close operation durations.
    pub const fn operation_config(mut self, config: GroupConsumerOperationConfig) -> Self {
        self.operations = config;
        self
    }

    /// Replaces the end-to-end duration for each later [`crate::Consumer::seek`].
    pub const fn seek_timeout(mut self, timeout: Duration) -> Self {
        self.operations = self.operations.with_seek_timeout(timeout);
        self
    }

    /// Replaces the end-to-end duration for explicit or requested group close.
    pub const fn close_timeout(mut self, timeout: Duration) -> Self {
        self.operations = self.operations.with_close_timeout(timeout);
        self
    }

    /// Sets the broker Fetch policy for this consumer registration.
    pub const fn fetch_config(mut self, fetch: ConsumerFetchConfig) -> Self {
        self.fetch = fetch;
        self
    }

    /// Sets bounded Fetch-call and retained-delivery capacities.
    pub const fn limits(mut self, limits: ConsumerLimits) -> Self {
        self.limits = limits;
        self
    }

    /// Returns the requested Kafka group spelling.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Returns the requested static member identity, when configured.
    pub fn selected_group_instance_id(&self) -> Option<&str> {
        self.group_instance_id.as_deref()
    }

    /// Returns the caller-ordered requested subscription.
    pub fn subscription(&self) -> &[String] {
        &self.topics
    }

    /// Returns the selected Kafka consumer-group membership protocol.
    pub const fn selected_group_protocol(&self) -> ConsumerGroupProtocol {
        self.group_protocol
    }

    /// Returns the effective classic-group partition assignor, when applicable.
    pub const fn selected_classic_group_assignor(&self) -> Option<ClassicGroupAssignor> {
        match (self.group_protocol, self.classic_group_assignor) {
            (ConsumerGroupProtocol::Classic, Some(assignor)) => Some(assignor),
            (ConsumerGroupProtocol::Classic, None) => Some(ClassicGroupAssignor::Range),
            (ConsumerGroupProtocol::Consumer, _) => None,
        }
    }

    /// Returns the requested missing-offset policy.
    pub const fn offset_reset(&self) -> OffsetReset {
        self.offset_reset
    }

    /// Returns the immutable record-visibility policy for this registration.
    pub const fn selected_read_isolation(&self) -> ReadIsolation {
        self.read_isolation
    }

    /// Returns the requested application-processing timeout.
    pub const fn selected_processing_timeout(&self) -> Duration {
        self.processing_timeout
    }

    /// Returns the requested membership-start operation timeout.
    pub const fn selected_membership_start_timeout(&self) -> Duration {
        self.membership_start_timeout
    }

    /// Returns the requested classic-membership timing.
    pub const fn selected_classic_group_config(&self) -> ClassicGroupConfig {
        self.classic_group_config
    }

    /// Returns the requested hosted-group operation durations.
    pub const fn selected_operation_config(&self) -> GroupConsumerOperationConfig {
        self.operations
    }

    /// Returns the requested end-to-end duration for each later group seek.
    pub const fn selected_seek_timeout(&self) -> Duration {
        self.operations.seek_timeout()
    }

    /// Returns the requested end-to-end duration for explicit or requested close.
    pub const fn selected_close_timeout(&self) -> Duration {
        self.operations.close_timeout()
    }

    /// Returns the requested broker Fetch policy.
    pub const fn selected_fetch_config(&self) -> ConsumerFetchConfig {
        self.fetch
    }

    /// Returns the requested Fetch-call and retained-delivery capacities.
    pub const fn selected_limits(&self) -> ConsumerLimits {
        self.limits
    }
}
