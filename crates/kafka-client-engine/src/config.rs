//! Public engine defaults compiled into bounded producer-host settings.

use std::time::Duration;

use kafka_client_core::{ByteCount, ProducerBatchPolicy};

use crate::producer::{ProducerHostLimitError, ProducerHostLimits, host_turn::ProducerTurnBudget};

mod producer_limits;
pub use producer_limits::EngineProducerLimits;

#[cfg(test)]
mod producer_limits_test;

const DEFAULT_DELIVERY_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_TURN_BUDGET: usize = 64;

/// Engine construction inputs compiled before any host thread starts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineConfig {
    bootstrap_servers: Vec<String>,
    delivery_timeout: Duration,
    producer_limits: EngineProducerLimits,
}

impl EngineConfig {
    /// Creates an engine configuration with engine-owned producer defaults.
    pub fn new(bootstrap_servers: Vec<String>) -> Self {
        Self {
            bootstrap_servers,
            delivery_timeout: DEFAULT_DELIVERY_TIMEOUT,
            producer_limits: EngineProducerLimits::default(),
        }
    }

    /// Replaces the end-to-end default producer delivery timeout.
    #[must_use]
    pub const fn with_delivery_timeout(mut self, delivery_timeout: Duration) -> Self {
        self.delivery_timeout = delivery_timeout;
        self
    }

    /// Replaces the provisional bounded producer resource contract.
    #[must_use]
    pub const fn with_producer_limits(mut self, producer_limits: EngineProducerLimits) -> Self {
        self.producer_limits = producer_limits;
        self
    }

    /// Returns configured logical bootstrap endpoints.
    pub fn bootstrap_servers(&self) -> &[String] {
        &self.bootstrap_servers
    }

    /// Returns the engine-owned default delivery timeout.
    pub const fn delivery_timeout(&self) -> Duration {
        self.delivery_timeout
    }

    /// Returns the provisional bounded producer limits.
    pub const fn producer_limits(&self) -> EngineProducerLimits {
        self.producer_limits
    }

    pub(crate) fn validate(&self) -> Result<ValidatedEngineConfig, EngineConfigError> {
        if self.bootstrap_servers.is_empty() {
            return Err(EngineConfigError::EmptyBootstrap);
        }
        if self.delivery_timeout.is_zero() {
            return Err(EngineConfigError::ZeroDeliveryTimeout);
        }
        duration_ticks(self.delivery_timeout)?;
        let host_limits = self.producer_host_limits()?;
        let validated_host = host_limits
            .validate()
            .map_err(EngineConfigError::Producer)?;
        drop(validated_host);
        let Some(turn_budget) = ProducerTurnBudget::try_new(
            DEFAULT_TURN_BUDGET,
            DEFAULT_TURN_BUDGET,
            DEFAULT_TURN_BUDGET,
            DEFAULT_TURN_BUDGET,
            DEFAULT_TURN_BUDGET,
        ) else {
            return Err(EngineConfigError::TurnBudget);
        };
        Ok(ValidatedEngineConfig {
            host_limits,
            turn_budget,
        })
    }

    fn producer_host_limits(&self) -> Result<ProducerHostLimits, EngineConfigError> {
        let limits = self.producer_limits;
        let _retained_bytes =
            u64::try_from(limits.retained_bytes()).map_err(|_| EngineConfigError::RetainedBytes)?;
        let batch_bytes =
            u64::try_from(limits.batch_bytes()).map_err(|_| EngineConfigError::BatchBytes)?;
        let linger_ticks = duration_ticks(limits.linger())?;
        let batch_policy = ProducerBatchPolicy::try_new(
            limits.batch_records(),
            ByteCount::new(batch_bytes),
            linger_ticks,
        )
        .map_err(|_| EngineConfigError::BatchPolicy)?;
        Ok(ProducerHostLimits {
            retained_bytes: limits.retained_bytes(),
            completion_capacity: limits.in_flight_records(),
            record_capacity: limits.in_flight_records(),
            batch_capacity: limits.in_flight_records(),
            timer_capacity: limits.in_flight_records(),
            encoded_byte_capacity: limits.retained_bytes(),
            max_wire_batch_bytes: limits.batch_bytes(),
            batch_policy,
        })
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ValidatedEngineConfig {
    pub(crate) host_limits: ProducerHostLimits,
    pub(crate) turn_budget: ProducerTurnBudget,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EngineConfigError {
    EmptyBootstrap,
    ZeroDeliveryTimeout,
    DurationOverflow,
    RetainedBytes,
    BatchBytes,
    BatchPolicy,
    Producer(ProducerHostLimitError),
    TurnBudget,
}

fn duration_ticks(duration: Duration) -> Result<u64, EngineConfigError> {
    u64::try_from(duration.as_nanos()).map_err(|_| EngineConfigError::DurationOverflow)
}
