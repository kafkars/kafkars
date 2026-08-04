//! Inert public protocol and policy for one bounded group-consumer registration.

use std::{sync::Arc, time::Duration};

use kafka_client_core::{
    ClassicProcessingLeasePolicy, ClassicProtocol, GroupPositionMissingOffsetPolicy,
};

use crate::config::{
    ConsumerReadIsolation, EngineClassicGroupConfig, EngineConsumerFetchConfig,
    EngineConsumerLimits, EngineGroupConsumerOperationConfig, ValidatedClassicGroupConfig,
    ValidatedConsumerFetchConfig, ValidatedConsumerLimits, ValidatedGroupConsumerOperationConfig,
    validate_consumer_fetch_envelope,
};

const DEFAULT_PROCESSING_TIMEOUT: Duration = Duration::from_secs(300);

type ValidatedGroupConsumerRegistration = (
    Arc<str>,
    Option<Arc<str>>,
    Vec<Arc<str>>,
    GroupConsumerProtocol,
    Option<GroupConsumerClassicAssignor>,
    Option<GroupConsumerClassicAssignor>,
    GroupConsumerMissingOffsetPolicy,
    ConsumerReadIsolation,
    ClassicProcessingLeasePolicy,
    EngineClassicGroupConfig,
    ValidatedClassicGroupConfig,
    EngineGroupConsumerOperationConfig,
    ValidatedGroupConsumerOperationConfig,
    EngineConsumerFetchConfig,
    ValidatedConsumerFetchConfig,
    EngineConsumerLimits,
    ValidatedConsumerLimits,
);

/// Exact caller-owned policy for one bounded group-consumer registration.
#[derive(Debug)]
pub struct GroupConsumerRegistration {
    group: Arc<str>,
    group_instance_id: Option<Arc<str>>,
    topics: Vec<Arc<str>>,
    protocol: GroupConsumerProtocol,
    classic_assignor: Option<GroupConsumerClassicAssignor>,
    missing_offset_policy: GroupConsumerMissingOffsetPolicy,
    read_isolation: ConsumerReadIsolation,
    processing_timeout: Duration,
    classic_group_config: EngineClassicGroupConfig,
    operation_config: EngineGroupConsumerOperationConfig,
    fetch: EngineConsumerFetchConfig,
    limits: EngineConsumerLimits,
}

impl GroupConsumerRegistration {
    /// Creates an inert registration request without starting membership work.
    pub fn new(group: Arc<str>, topics: Vec<Arc<str>>) -> Self {
        Self {
            group,
            group_instance_id: None,
            topics,
            protocol: GroupConsumerProtocol::Classic,
            classic_assignor: None,
            missing_offset_policy: GroupConsumerMissingOffsetPolicy::Error,
            read_isolation: ConsumerReadIsolation::ReadUncommitted,
            processing_timeout: DEFAULT_PROCESSING_TIMEOUT,
            classic_group_config: EngineClassicGroupConfig::default(),
            operation_config: EngineGroupConsumerOperationConfig::default(),
            fetch: EngineConsumerFetchConfig::default(),
            limits: EngineConsumerLimits::default(),
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

    /// Selects the Kafka consumer-group membership protocol.
    pub const fn with_protocol(mut self, protocol: GroupConsumerProtocol) -> Self {
        self.protocol = protocol;
        self
    }

    /// Returns the selected Kafka consumer-group membership protocol.
    pub const fn protocol(&self) -> GroupConsumerProtocol {
        self.protocol
    }

    /// Selects the classic-group partition assignor.
    ///
    /// Combining this with [`GroupConsumerProtocol::Consumer`] is invalid.
    pub const fn with_classic_assignor(mut self, assignor: GroupConsumerClassicAssignor) -> Self {
        self.classic_assignor = Some(assignor);
        self
    }

    /// Returns the effective classic-group partition assignor, when applicable.
    pub const fn classic_assignor(&self) -> Option<GroupConsumerClassicAssignor> {
        match (self.protocol, self.classic_assignor) {
            (GroupConsumerProtocol::Classic, Some(assignor)) => Some(assignor),
            (GroupConsumerProtocol::Classic, None) => Some(GroupConsumerClassicAssignor::Range),
            (GroupConsumerProtocol::Consumer, _) => None,
        }
    }

    /// Selects explicit missing-committed-offset behavior before registration.
    pub const fn with_missing_offset_policy(
        mut self,
        policy: GroupConsumerMissingOffsetPolicy,
    ) -> Self {
        self.missing_offset_policy = policy;
        self
    }

    /// Returns the immutable missing-offset policy carried into membership.
    pub const fn missing_offset_policy(&self) -> GroupConsumerMissingOffsetPolicy {
        self.missing_offset_policy
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

    /// Replaces immutable classic membership timing for this registration.
    pub const fn with_classic_group_config(mut self, config: EngineClassicGroupConfig) -> Self {
        self.classic_group_config = config;
        self
    }

    /// Returns raw classic membership timing for this registration.
    pub const fn classic_group_config(&self) -> EngineClassicGroupConfig {
        self.classic_group_config
    }

    /// Replaces hosted-group seek and close durations for this registration.
    pub const fn with_operation_config(
        mut self,
        config: EngineGroupConsumerOperationConfig,
    ) -> Self {
        self.operation_config = config;
        self
    }

    /// Returns raw hosted-group seek and close durations.
    pub const fn operation_config(&self) -> EngineGroupConsumerOperationConfig {
        self.operation_config
    }

    /// Replaces the broker Fetch policy for this group registration.
    pub const fn with_fetch(mut self, fetch: EngineConsumerFetchConfig) -> Self {
        self.fetch = fetch;
        self
    }

    /// Returns the raw broker Fetch policy for this group registration.
    pub const fn fetch(&self) -> EngineConsumerFetchConfig {
        self.fetch
    }

    /// Replaces bounded Fetch-call and delivery ownership for this group.
    pub const fn with_limits(mut self, limits: EngineConsumerLimits) -> Self {
        self.limits = limits;
        self
    }

    /// Returns raw Fetch-call and delivery ownership for this group.
    pub const fn limits(&self) -> EngineConsumerLimits {
        self.limits
    }

    pub(super) fn into_validated_parts(self) -> Result<ValidatedGroupConsumerRegistration, Self> {
        if self.protocol == GroupConsumerProtocol::Consumer
            && self.classic_group_config != EngineClassicGroupConfig::default()
        {
            return Err(self);
        }
        let effective_classic_assignor = match (self.protocol, self.classic_assignor) {
            (GroupConsumerProtocol::Classic, Some(assignor)) => Some(assignor),
            (GroupConsumerProtocol::Classic, None) => Some(GroupConsumerClassicAssignor::Range),
            (GroupConsumerProtocol::Consumer, None) => None,
            (GroupConsumerProtocol::Consumer, Some(_)) => return Err(self),
        };
        let timeout_ticks = match u64::try_from(self.processing_timeout.as_nanos()) {
            Ok(timeout_ticks) => timeout_ticks,
            Err(_overflow) => return Err(self),
        };
        let processing_policy = match ClassicProcessingLeasePolicy::try_new(timeout_ticks) {
            Ok(policy) => policy,
            Err(_invalid) => return Err(self),
        };
        let validated_classic_group_config = match self.classic_group_config.validate() {
            Ok(config) => config,
            Err(_invalid) => return Err(self),
        };
        let validated_operation_config = match self.operation_config.validate() {
            Ok(config) => config,
            Err(_invalid) => return Err(self),
        };
        let validated_fetch = match self.fetch.validate() {
            Ok(fetch) => fetch,
            Err(_invalid) => return Err(self),
        };
        let validated_limits = match self.limits.validate() {
            Ok(limits) => limits,
            Err(_invalid) => return Err(self),
        };
        if validate_consumer_fetch_envelope(validated_limits, validated_fetch.partition_max_bytes())
            .is_err()
        {
            return Err(self);
        }
        Ok((
            self.group,
            self.group_instance_id,
            self.topics,
            self.protocol,
            effective_classic_assignor,
            self.classic_assignor,
            self.missing_offset_policy,
            self.read_isolation,
            processing_policy,
            self.classic_group_config,
            validated_classic_group_config,
            self.operation_config,
            validated_operation_config,
            self.fetch,
            validated_fetch,
            self.limits,
            validated_limits,
        ))
    }
}

/// Kafka consumer-group membership protocol selected before registration.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum GroupConsumerProtocol {
    /// Join, Sync, and Heartbeat through Kafka's classic group protocol.
    #[default]
    Classic,
    /// Join and maintain membership through KIP-848 `ConsumerGroupHeartbeat`.
    Consumer,
}

/// Classic consumer-group partition assignor selected before registration.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum GroupConsumerClassicAssignor {
    /// Kafka's eager `Range` assignor.
    #[default]
    Range,
    /// Kafka's incremental `CooperativeSticky` assignor.
    CooperativeSticky,
}

impl GroupConsumerClassicAssignor {
    pub(super) const fn into_core(self) -> ClassicProtocol {
        match self {
            Self::Range => ClassicProtocol::Range,
            Self::CooperativeSticky => ClassicProtocol::CooperativeSticky,
        }
    }
}

/// Missing committed-offset behavior fixed for one registered group.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum GroupConsumerMissingOffsetPolicy {
    /// Fail the complete assignment atomically.
    #[default]
    Error,
    /// Resolve each missing partition to Kafka's earliest available offset.
    Earliest,
    /// Resolve each missing partition to Kafka's latest available offset.
    Latest,
}

impl GroupConsumerMissingOffsetPolicy {
    pub(super) const fn into_core(self) -> GroupPositionMissingOffsetPolicy {
        match self {
            Self::Error => GroupPositionMissingOffsetPolicy::Error,
            Self::Earliest => GroupPositionMissingOffsetPolicy::Earliest,
            Self::Latest => GroupPositionMissingOffsetPolicy::Latest,
        }
    }
}
