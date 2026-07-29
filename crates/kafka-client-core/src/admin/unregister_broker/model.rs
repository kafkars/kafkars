//! Validated single-broker intent for one `UnregisterBroker` request.

use core::fmt;

/// Validated intent for one destructive controller `UnregisterBroker` RPC.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UnregisterBrokerPlan {
    broker_id: i32,
}

impl UnregisterBrokerPlan {
    /// Validates one nonnegative Kafka broker identity.
    pub const fn new(broker_id: i32) -> Result<Self, UnregisterBrokerPlanError> {
        if broker_id < 0 {
            return Err(UnregisterBrokerPlanError::NegativeBrokerId);
        }
        Ok(Self { broker_id })
    }

    /// Returns the exact broker identity to unregister.
    pub const fn broker_id(self) -> i32 {
        self.broker_id
    }

    /// Consumes this plan into its adapter-owned scalar.
    pub const fn into_broker_id(self) -> i32 {
        self.broker_id
    }
}

/// Invalid deterministic broker-unregistration intent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnregisterBrokerPlanError {
    /// Kafka broker identities cannot be negative.
    NegativeBrokerId,
}

impl fmt::Display for UnregisterBrokerPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid UnregisterBroker plan: {self:?}")
    }
}

impl std::error::Error for UnregisterBrokerPlanError {}
