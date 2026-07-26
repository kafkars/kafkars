//! Fair transaction-initialization execution within the integrated host.

use kafka_client_core::Deadline;

use super::{EngineHostError, EngineHostResources};
use crate::transaction::{TransactionInitializationShardLockError, TransactionInitializationTurn};

pub(super) struct TransactionInitializationProgress {
    pub(super) unsettled: usize,
    pub(super) progressed: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    now: kafka_client_core::Moment,
) -> Result<TransactionInitializationProgress, EngineHostError> {
    let mut host = match resources.transaction_initialization.try_host() {
        Ok(host) => host,
        Err(TransactionInitializationShardLockError::Contended) => {
            return Ok(TransactionInitializationProgress {
                unsettled: usize::MAX,
                progressed: false,
                next_deadline: None,
            });
        }
        Err(TransactionInitializationShardLockError::Poisoned) => {
            return Err(EngineHostError::TransactionInitializationLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        resources.transaction_initialization.close_locked(&mut host);
    }
    let driver = resources
        .driver
        .as_ref()
        .ok_or(EngineHostError::DriverOwnerMissing)?;
    let turn = host
        .turn(now, driver)
        .map_err(EngineHostError::TransactionInitialization)?;
    Ok(TransactionInitializationProgress {
        unsettled: host.unsettled(),
        progressed: turn == TransactionInitializationTurn::Progress,
        next_deadline: host.next_deadline(),
    })
}
