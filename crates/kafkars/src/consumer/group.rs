//! Bounded classic-group registration without premature delivery exposure.

use std::time::Duration;

use crate::bridge::ClientEngine;
use crate::{ErrorKind, KafkaError};

use super::{
    ClassicGroupConfig, Consumer, ConsumerBuildError, ConsumerFetchConfig, ConsumerLimits,
    GroupConsumerOperationConfig, OffsetReset, ReadIsolation,
};

mod configuration;
#[cfg(test)]
mod configuration_test;

const DEFAULT_MEMBERSHIP_START_TIMEOUT: Duration = Duration::from_secs(30);

/// Builder for one bounded group-consumer registration.
#[derive(Debug, Clone)]
pub struct ConsumerBuilder {
    engine: ClientEngine,
    group_id: String,
    group_instance_id: Option<String>,
    topics: Vec<String>,
    group_protocol: ConsumerGroupProtocol,
    classic_group_assignor: Option<ClassicGroupAssignor>,
    offset_reset: OffsetReset,
    read_isolation: ReadIsolation,
    processing_timeout: Duration,
    membership_start_timeout: Duration,
    classic_group_config: ClassicGroupConfig,
    operations: GroupConsumerOperationConfig,
    fetch: ConsumerFetchConfig,
    limits: ConsumerLimits,
}

impl ConsumerBuilder {
    /// Registers this group and begins real hosted membership.
    ///
    /// The membership deadline is captured at this call boundary before
    /// validation or name conversion. A true pre-core rejection releases the
    /// fresh registration and returns this exact builder.
    #[expect(
        clippy::result_large_err,
        reason = "pre-admission rejection returns the exact consumed consumer builder"
    )]
    pub fn build(self) -> Result<Consumer, ConsumerBuildError> {
        let capture = match self
            .engine
            .capture_group_consumer_start(self.membership_start_timeout)
        {
            Ok(capture) => capture,
            Err(error) => return Err(ConsumerBuildError::new(self, error)),
        };
        if self.group_protocol == ConsumerGroupProtocol::Consumer
            && self.classic_group_assignor.is_some()
        {
            drop(capture);
            return Err(ConsumerBuildError::new(
                self,
                KafkaError::new(
                    ErrorKind::Configuration,
                    "a classic group assignor cannot be selected with the KIP-848 consumer-group protocol",
                ),
            ));
        }
        let classic_group_assignor = self.selected_classic_group_assignor();
        let engine = match self.engine.register_group_consumer(
            capture,
            &self.group_id,
            self.group_instance_id.as_deref(),
            &self.topics,
            self.group_protocol,
            classic_group_assignor,
            self.offset_reset,
            self.read_isolation,
            self.processing_timeout,
            self.classic_group_config,
            self.operations,
            self.fetch,
            self.limits,
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

/// Kafka consumer-group membership protocol selected for one consumer.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ConsumerGroupProtocol {
    /// Kafka's `JoinGroup`, `SyncGroup`, and `Heartbeat` protocol.
    #[default]
    Classic,
    /// Kafka's KIP-848 `ConsumerGroupHeartbeat` protocol.
    Consumer,
}

/// Classic consumer-group partition assignor selected for one consumer.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ClassicGroupAssignor {
    /// Kafka's eager `Range` assignor.
    #[default]
    Range,
    /// Kafka's incremental `CooperativeSticky` assignor.
    CooperativeSticky,
}
