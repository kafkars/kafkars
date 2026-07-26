//! Closed engine-configuration failures and checked duration normalization.

use std::time::Duration;

use kafka_client_core::ProducerRetryPolicyError;

use crate::producer::ProducerHostLimitError;

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

pub(super) fn duration_ticks(duration: Duration) -> Result<u64, EngineConfigError> {
    u64::try_from(duration.as_nanos()).map_err(|_| EngineConfigError::DurationOverflow)
}
