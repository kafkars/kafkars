//! Public engine defaults compiled into bounded producer-host settings.

use std::time::Duration;

use kafka_client_core::{ByteCount, ProducerBatchPolicy};

use crate::producer::{ProducerHostLimitError, ProducerHostLimits, host_turn::ProducerTurnBudget};

const DEFAULT_DELIVERY_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_RETAINED_BYTES: usize = 32 * 1024 * 1024;
const DEFAULT_IN_FLIGHT_RECORDS: usize = 1_024;
const DEFAULT_BATCH_RECORDS: usize = 256;
const DEFAULT_BATCH_BYTES: usize = 1024 * 1024;
const DEFAULT_LINGER: Duration = Duration::from_millis(5);
const DEFAULT_TURN_BUDGET: usize = 64;

/// Provisional bounded producer resources owned by the engine.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EngineProducerLimits {
    retained_bytes: usize,
    in_flight_records: usize,
    batch_records: usize,
    batch_bytes: usize,
    linger: Duration,
}

impl EngineProducerLimits {
    /// Creates an explicit provisional resource contract.
    pub const fn new(
        retained_bytes: usize,
        in_flight_records: usize,
        batch_records: usize,
        batch_bytes: usize,
        linger: Duration,
    ) -> Self {
        Self {
            retained_bytes,
            in_flight_records,
            batch_records,
            batch_bytes,
            linger,
        }
    }

    /// Returns the maximum retained application bytes.
    pub const fn retained_bytes(self) -> usize {
        self.retained_bytes
    }

    /// Returns the accepted record and terminal-completion capacity.
    pub const fn in_flight_records(self) -> usize {
        self.in_flight_records
    }

    /// Returns the maximum records in one accumulator.
    pub const fn batch_records(self) -> usize {
        self.batch_records
    }

    /// Returns the maximum encoded bytes retained for one Produce batch.
    pub const fn batch_bytes(self) -> usize {
        self.batch_bytes
    }

    /// Returns the engine-owned linger duration.
    pub const fn linger(self) -> Duration {
        self.linger
    }
}

impl Default for EngineProducerLimits {
    fn default() -> Self {
        Self::new(
            DEFAULT_RETAINED_BYTES,
            DEFAULT_IN_FLIGHT_RECORDS,
            DEFAULT_BATCH_RECORDS,
            DEFAULT_BATCH_BYTES,
            DEFAULT_LINGER,
        )
    }
}

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
        let notification_capacity = limits
            .in_flight_records
            .checked_add(limits.in_flight_records)
            .ok_or(EngineConfigError::NotificationCapacity)?;
        let _retained_bytes =
            u64::try_from(limits.retained_bytes).map_err(|_| EngineConfigError::RetainedBytes)?;
        let batch_bytes =
            u64::try_from(limits.batch_bytes).map_err(|_| EngineConfigError::BatchBytes)?;
        let linger_ticks = duration_ticks(limits.linger)?;
        let batch_policy = ProducerBatchPolicy::try_new(
            limits.batch_records,
            ByteCount::new(batch_bytes),
            linger_ticks,
        )
        .map_err(|_| EngineConfigError::BatchPolicy)?;
        Ok(ProducerHostLimits {
            retained_bytes: limits.retained_bytes,
            completion_capacity: limits.in_flight_records,
            record_capacity: limits.in_flight_records,
            batch_capacity: limits.in_flight_records,
            timer_capacity: limits.in_flight_records,
            pending_notification_capacity: limits.in_flight_records,
            notification_capacity,
            encoded_byte_capacity: limits.retained_bytes,
            max_wire_batch_bytes: limits.batch_bytes,
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
    NotificationCapacity,
    Producer(ProducerHostLimitError),
    TurnBudget,
}

fn duration_ticks(duration: Duration) -> Result<u64, EngineConfigError> {
    u64::try_from(duration.as_nanos()).map_err(|_| EngineConfigError::DurationOverflow)
}
