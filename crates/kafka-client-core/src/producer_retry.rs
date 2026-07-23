//! Deterministic retry limits and normalized definitely-unsent failure facts.

use core::fmt;

/// Structural reason one Produce attempt ended outside broker policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProducerAttemptFailureKind {
    /// A bounded local owner could not retain the attempt.
    LocalCapacity,
    /// The driver had no current partition route.
    RouteUnavailable,
    /// The driver could not resolve a required broker name.
    NameResolutionUnavailable,
    /// A usable connection generation was temporarily unavailable.
    ConnectionUnavailable,
    /// Retrying cannot repair the structural failure or is not yet supported.
    Permanent,
}

impl ProducerAttemptFailureKind {
    /// Returns whether policy may retry after authoritative `NotSent` evidence.
    pub const fn is_structurally_transient(self) -> bool {
        matches!(
            self,
            Self::LocalCapacity
                | Self::RouteUnavailable
                | Self::NameResolutionUnavailable
                | Self::ConnectionUnavailable
        )
    }
}

/// Fixed attempt bound and deterministic retry delay.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerRetryPolicy {
    max_retries: u32,
    backoff_ticks: u64,
}

/// Invalid bounded retry policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProducerRetryPolicyError {
    /// Enabled retry requires a nonzero delay to avoid immediate host churn.
    ZeroBackoff,
}

impl fmt::Display for ProducerRetryPolicyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("enabled producer retry backoff must be nonzero")
    }
}

impl std::error::Error for ProducerRetryPolicyError {}

impl ProducerRetryPolicy {
    /// Disables automatic retry while preserving explicit terminal policy.
    pub const fn none() -> Self {
        Self {
            max_retries: 0,
            backoff_ticks: 0,
        }
    }

    /// Creates a bounded retry policy over the original operation deadline.
    pub const fn try_fixed(
        max_retries: u32,
        backoff_ticks: u64,
    ) -> Result<Self, ProducerRetryPolicyError> {
        if max_retries == 0 {
            return Ok(Self::none());
        }
        if backoff_ticks == 0 {
            return Err(ProducerRetryPolicyError::ZeroBackoff);
        }
        Ok(Self {
            max_retries,
            backoff_ticks,
        })
    }

    /// Returns the number of replacement executions policy may start.
    pub const fn max_retries(self) -> u32 {
        self.max_retries
    }

    /// Returns the deterministic delay before each replacement execution.
    pub const fn backoff_ticks(self) -> u64 {
        self.backoff_ticks
    }
}
