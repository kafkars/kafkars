//! Fallible consumer-registry allocation before transaction-host construction.

use crate::consumer::{GroupConsumerRegistry, ShareConsumerRegistry};

use super::super::EngineStartError;

pub(super) fn start() -> Result<(GroupConsumerRegistry, ShareConsumerRegistry), EngineStartError> {
    let group =
        GroupConsumerRegistry::start().map_err(|error| EngineStartError::group_consumer(&error))?;
    let share =
        ShareConsumerRegistry::start().map_err(|error| EngineStartError::share_consumer(&error))?;
    Ok((group, share))
}
