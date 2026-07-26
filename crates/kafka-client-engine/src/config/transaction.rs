//! Engine-owned defaults for transaction initialization.

use std::time::Duration;

use super::EngineConfig;

const DEFAULT_INITIALIZATION_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_TRANSACTION_TIMEOUT: Duration = Duration::from_secs(60);

impl EngineConfig {
    /// Returns the engine-owned default transaction-initialization deadline.
    pub const fn transaction_initialization_timeout(&self) -> Duration {
        DEFAULT_INITIALIZATION_TIMEOUT
    }

    /// Returns the engine-owned default broker transaction timeout.
    pub const fn transaction_timeout(&self) -> Duration {
        DEFAULT_TRANSACTION_TIMEOUT
    }
}
