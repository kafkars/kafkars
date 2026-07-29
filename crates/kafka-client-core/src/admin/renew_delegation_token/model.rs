//! Linear HMAC and exact broker-period intent for API 39.

use core::fmt;

use super::RenewDelegationTokenHmac;

/// Validated intent for one single-attempt token renewal.
#[derive(Debug, Eq, PartialEq)]
pub struct RenewDelegationTokenPlan {
    hmac: RenewDelegationTokenHmac,
    broker_renew_period_ms: i64,
}

impl RenewDelegationTokenPlan {
    /// Validates one unique HMAC and optional positive renewal period.
    ///
    /// Absence is retained as Kafka's exact `-1` broker-default sentinel.
    pub fn new(
        hmac: RenewDelegationTokenHmac,
        renew_period_ms: Option<i64>,
    ) -> Result<Self, RenewDelegationTokenPlanError> {
        let broker_renew_period_ms = match renew_period_ms {
            None => -1,
            Some(value) if value > 0 => value,
            Some(_) => return Err(RenewDelegationTokenPlanError::NonPositiveRenewPeriod),
        };
        Ok(Self {
            hmac,
            broker_renew_period_ms,
        })
    }

    /// Borrows the uniquely owned request HMAC.
    pub const fn hmac(&self) -> &RenewDelegationTokenHmac {
        &self.hmac
    }

    /// Returns the explicit positive renewal period, if supplied.
    pub const fn renew_period_ms(&self) -> Option<i64> {
        if self.broker_renew_period_ms == -1 {
            None
        } else {
            Some(self.broker_renew_period_ms)
        }
    }

    /// Returns the exact signed Kafka field, including `-1` for broker default.
    pub const fn broker_renew_period_ms(&self) -> i64 {
        self.broker_renew_period_ms
    }

    /// Consumes the plan into its unique HMAC and exact signed Kafka period.
    pub fn into_parts(self) -> (RenewDelegationTokenHmac, i64) {
        (self.hmac, self.broker_renew_period_ms)
    }
}

/// Invalid deterministic token-renewal intent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RenewDelegationTokenPlanError {
    /// Token HMACs cannot be empty.
    EmptyHmac,
    /// The token HMAC exceeded the deterministic retained bound.
    HmacTooLong,
    /// An explicit renewal period must be positive.
    NonPositiveRenewPeriod,
}

impl fmt::Display for RenewDelegationTokenPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid RenewDelegationToken plan: {self:?}")
    }
}

impl std::error::Error for RenewDelegationTokenPlanError {}
