//! Linear HMAC and exact broker-period intent for API 40.

use core::fmt;

use super::ExpireDelegationTokenHmac;

/// Validated intent for one single-attempt token expiration.
#[derive(Debug, Eq, PartialEq)]
pub struct ExpireDelegationTokenPlan {
    hmac: ExpireDelegationTokenHmac,
    broker_expiry_period_ms: i64,
}

impl ExpireDelegationTokenPlan {
    /// Validates one unique HMAC and optional nonnegative expiry period.
    ///
    /// Absence is retained as Kafka's exact `-1` immediate-expiry default.
    pub fn new(
        hmac: ExpireDelegationTokenHmac,
        expiry_period_ms: Option<i64>,
    ) -> Result<Self, ExpireDelegationTokenPlanError> {
        let broker_expiry_period_ms = match expiry_period_ms {
            None => -1,
            Some(value) if value >= 0 => value,
            Some(_) => return Err(ExpireDelegationTokenPlanError::NegativeExpiryPeriod),
        };
        Ok(Self {
            hmac,
            broker_expiry_period_ms,
        })
    }

    /// Borrows the uniquely owned request HMAC.
    pub const fn hmac(&self) -> &ExpireDelegationTokenHmac {
        &self.hmac
    }

    /// Returns the explicit nonnegative expiry period, if supplied.
    pub const fn expiry_period_ms(&self) -> Option<i64> {
        if self.broker_expiry_period_ms == -1 {
            None
        } else {
            Some(self.broker_expiry_period_ms)
        }
    }

    /// Returns the exact signed Kafka field, including `-1` for immediate expiry.
    pub const fn broker_expiry_period_ms(&self) -> i64 {
        self.broker_expiry_period_ms
    }

    /// Consumes the plan into its unique HMAC and exact signed Kafka period.
    pub fn into_parts(self) -> (ExpireDelegationTokenHmac, i64) {
        (self.hmac, self.broker_expiry_period_ms)
    }
}

/// Invalid deterministic token-expiration intent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExpireDelegationTokenPlanError {
    /// Token HMACs cannot be empty.
    EmptyHmac,
    /// The token HMAC exceeded the deterministic retained bound.
    HmacTooLong,
    /// An explicit expiry period cannot be negative.
    NegativeExpiryPeriod,
}

impl fmt::Display for ExpireDelegationTokenPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid ExpireDelegationToken plan: {self:?}")
    }
}

impl std::error::Error for ExpireDelegationTokenPlanError {}
