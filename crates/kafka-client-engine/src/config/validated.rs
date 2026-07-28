//! Validated producer resources ready for deterministic host startup.

use crate::producer::{ProducerHostLimits, host_turn::ProducerTurnBudget};

#[derive(Clone, Copy, Debug)]
pub(crate) struct ValidatedEngineConfig {
    pub(crate) host_limits: ProducerHostLimits,
    pub(crate) turn_budget: ProducerTurnBudget,
}
