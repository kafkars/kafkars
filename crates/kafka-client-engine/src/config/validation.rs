//! Closed engine-configuration failures and checked duration normalization.

use std::time::Duration;

use kafka_client_core::{
    ByteCount, ProducerBatchPolicy, ProducerRetryPolicy, ProducerRetryPolicyError,
};

use crate::producer::{ProducerHostLimitError, ProducerHostLimits, host_turn::ProducerTurnBudget};

use super::{
    DEFAULT_COMPRESSION_WORKERS, DEFAULT_TURN_BUDGET, EngineConfig, ValidatedEngineConfig,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EngineConfigError {
    EmptyBootstrap,
    ZeroDeliveryTimeout,
    ZeroAdminTimeout,
    DurationOverflow,
    RetainedBytes,
    BatchBytes,
    CompressionBytes,
    BatchPolicy,
    RetryPolicy(ProducerRetryPolicyError),
    Producer(ProducerHostLimitError),
    TurnBudget,
}

impl EngineConfig {
    pub(crate) fn validate(&self) -> Result<ValidatedEngineConfig, EngineConfigError> {
        if self.bootstrap_servers().is_empty() {
            return Err(EngineConfigError::EmptyBootstrap);
        }
        if self.delivery_timeout().is_zero() {
            return Err(EngineConfigError::ZeroDeliveryTimeout);
        }
        duration_ticks(self.delivery_timeout())?;
        if self.admin_timeout().is_zero() {
            return Err(EngineConfigError::ZeroAdminTimeout);
        }
        duration_ticks(self.admin_timeout())?;
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
        let limits = self.producer_limits();
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
        let retry_policy = if self.producer_retry_max == 0 {
            ProducerRetryPolicy::none()
        } else {
            let retry_ticks = duration_ticks(self.producer_retry_backoff)?;
            ProducerRetryPolicy::try_fixed(self.producer_retry_max, retry_ticks)
                .map_err(EngineConfigError::RetryPolicy)?
        };
        let compression = self.producer_compression().core();
        let compressed = compression != kafka_client_core::CompressionPolicy::None;
        let compression_byte_capacity = if compressed {
            limits
                .retained_bytes()
                .checked_add(limits.retained_bytes())
                .ok_or(EngineConfigError::CompressionBytes)?
        } else {
            0
        };
        Ok(ProducerHostLimits {
            retained_bytes: limits.retained_bytes(),
            completion_capacity: limits.in_flight_records(),
            record_capacity: limits.in_flight_records(),
            batch_capacity: limits.in_flight_records(),
            timer_capacity: limits.in_flight_records(),
            encoded_byte_capacity: limits.retained_bytes(),
            max_wire_batch_bytes: limits.batch_bytes(),
            batch_policy,
            retry_policy,
            compression,
            compression_worker_count: if compressed {
                DEFAULT_COMPRESSION_WORKERS
            } else {
                0
            },
            compression_job_capacity: if compressed {
                limits.in_flight_records()
            } else {
                0
            },
            compression_byte_capacity,
        })
    }
}

pub(super) fn duration_ticks(duration: Duration) -> Result<u64, EngineConfigError> {
    u64::try_from(duration.as_nanos()).map_err(|_| EngineConfigError::DurationOverflow)
}
