//! Validated producer resources ready for deterministic host startup.

use crate::{
    driver::ValidatedSecurity,
    producer::{ProducerHostLimits, host_turn::ProducerTurnBudget},
};

#[derive(Debug)]
pub(crate) struct ValidatedEngineConfig {
    pub(crate) host_limits: ProducerHostLimits,
    pub(crate) turn_budget: ProducerTurnBudget,
    pub(crate) security: ValidatedSecurity,
}
