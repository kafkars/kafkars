//! Fair host turns for transaction-coordinator Admin `FenceProducers` work.

use kafka_client_core::{Deadline, Moment};

use crate::{
    admin::{AdminFenceProducersShardLockError, AdminFenceProducersTurn},
    driver::TransactionInitCall,
    protocol::transaction::remaining_fence_producer_timeout_ms,
};

use super::super::{EngineHostError, EngineHostResources};

pub(super) struct AdminFenceProducersProgress {
    pub(super) unsettled: usize,
    pub(super) driver_progress: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    now: Moment,
) -> Result<AdminFenceProducersProgress, EngineHostError> {
    let mut host = match resources.fence_producers.try_host() {
        Ok(host) => host,
        Err(AdminFenceProducersShardLockError::Contended) => {
            return Ok(AdminFenceProducersProgress::contended());
        }
        Err(AdminFenceProducersShardLockError::Poisoned) => {
            return Err(EngineHostError::AdminFenceProducersLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        resources.fence_producers.close_locked(&mut host);
    }
    let turn = host
        .turn(now)
        .map_err(EngineHostError::AdminFenceProducers)?;
    let driver_progress = match turn {
        AdminFenceProducersTurn::Idle => false,
        AdminFenceProducersTurn::Progress => true,
        AdminFenceProducersTurn::Submit(submission) => {
            let (operation_id, deadline, transactional_id) = submission.into_parts();
            let Ok(transaction_timeout_ms) =
                remaining_fence_producer_timeout_ms(now, deadline.core())
            else {
                host.expire_handoff(operation_id)
                    .map_err(EngineHostError::AdminFenceProducers)?;
                return Ok(AdminFenceProducersProgress {
                    unsettled: host.unsettled(),
                    driver_progress: true,
                    next_deadline: host.next_deadline(),
                });
            };
            let driver = resources
                .driver
                .as_ref()
                .ok_or(EngineHostError::DriverOwnerMissing)?;
            match TransactionInitCall::submit(
                driver,
                &transactional_id,
                transaction_timeout_ms,
                deadline.transport(),
            ) {
                Ok(call) => host
                    .accept_call(operation_id, call)
                    .map_err(EngineHostError::AdminFenceProducers)?,
                Err(_rejection) => {
                    host.reject_handoff(operation_id)
                        .map_err(EngineHostError::AdminFenceProducers)?;
                }
            }
            true
        }
    };
    Ok(AdminFenceProducersProgress {
        unsettled: host.unsettled(),
        driver_progress,
        next_deadline: host.next_deadline(),
    })
}

impl AdminFenceProducersProgress {
    const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        }
    }
}
