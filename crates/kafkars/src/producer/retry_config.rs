//! Public bounded definitely-unsent retry policy fixed before client startup.

use std::time::Duration;

/// Maximum replacement attempts and fixed delay for safe producer retries.
///
/// Only failures carrying authoritative definitely-unsent evidence are eligible
/// for replacement, and every replacement remains bounded by the original
/// record delivery deadline. The default permits three replacements with a
/// 100-millisecond backoff. A zero replacement count disables retry and makes
/// the selected backoff inert.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProducerRetryConfig {
    max_retries: u32,
    backoff: Duration,
}

impl ProducerRetryConfig {
    /// Creates one explicit definitely-unsent retry policy.
    pub const fn new(max_retries: u32, backoff: Duration) -> Self {
        Self {
            max_retries,
            backoff,
        }
    }

    /// Creates a policy that performs no replacement attempt.
    pub const fn disabled() -> Self {
        Self::new(0, Duration::ZERO)
    }

    /// Returns the maximum number of replacement attempts per Produce batch.
    pub const fn max_retries(self) -> u32 {
        self.max_retries
    }

    /// Returns the fixed delay before an eligible replacement attempt.
    pub const fn backoff(self) -> Duration {
        self.backoff
    }

    /// Replaces the maximum number of replacement attempts.
    #[must_use]
    pub const fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Replaces the fixed delay before an eligible replacement attempt.
    #[must_use]
    pub const fn with_backoff(mut self, backoff: Duration) -> Self {
        self.backoff = backoff;
        self
    }
}

impl Default for ProducerRetryConfig {
    fn default() -> Self {
        Self::new(3, Duration::from_millis(100))
    }
}
