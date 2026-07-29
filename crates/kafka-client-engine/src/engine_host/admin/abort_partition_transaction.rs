//! Fair host turns for one explicit leader-routed partition transaction abort.

use kafka_client_core::{Deadline, Moment};

use crate::{
    admin::{
        AbortPartitionTransactionShardLockError, AbortPartitionTransactionShardWake,
        AbortPartitionTransactionShardWakeError, AbortPartitionTransactionTurn,
    },
    driver::{AbortPartitionTransactionCall, ReactorWake},
};

use super::super::{EngineHostError, EngineHostResources};

pub(super) struct AbortPartitionTransactionProgress {
    pub(super) unsettled: usize,
    pub(super) driver_progress: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    now: Moment,
) -> Result<AbortPartitionTransactionProgress, EngineHostError> {
    let mut host = match resources.abort_partition_transaction.try_host() {
        Ok(host) => host,
        Err(AbortPartitionTransactionShardLockError::Contended) => {
            return Ok(AbortPartitionTransactionProgress::contended());
        }
        Err(AbortPartitionTransactionShardLockError::Poisoned) => {
            return Err(EngineHostError::AbortPartitionTransactionLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        resources
            .abort_partition_transaction
            .close_locked(&mut host);
    }
    let turn = host
        .turn(now)
        .map_err(EngineHostError::AbortPartitionTransaction)?;
    let driver_progress = match turn {
        AbortPartitionTransactionTurn::Idle => false,
        AbortPartitionTransactionTurn::Progress => true,
        AbortPartitionTransactionTurn::Submit(submission) => {
            let (operation_id, deadline, plan) = submission.into_parts();
            let driver = resources
                .driver
                .as_ref()
                .ok_or(EngineHostError::DriverOwnerMissing)?;
            match AbortPartitionTransactionCall::submit(driver, plan, deadline.transport()) {
                Ok(call) => host
                    .accept_call(operation_id, call)
                    .map_err(EngineHostError::AbortPartitionTransaction)?,
                Err(_rejection) => host
                    .reject_handoff(operation_id)
                    .map_err(EngineHostError::AbortPartitionTransaction)?,
            }
            true
        }
    };
    Ok(AbortPartitionTransactionProgress {
        unsettled: host.unsettled(),
        driver_progress,
        next_deadline: host.next_deadline(),
    })
}

impl AbortPartitionTransactionProgress {
    const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        }
    }
}

impl AbortPartitionTransactionShardWake for ReactorWake {
    fn wake(&self) -> Result<(), AbortPartitionTransactionShardWakeError> {
        self.request()
            .map_err(|error| AbortPartitionTransactionShardWakeError::from_io(error.into_io()))
    }
}
