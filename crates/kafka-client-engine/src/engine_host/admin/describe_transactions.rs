//! Fair host turns for transaction-coordinator Admin `DescribeTransactions` work.

use kafka_client_core::{Deadline, Moment};

use crate::{
    admin::{AdminDescribeTransactionsShardLockError, AdminDescribeTransactionsTurn},
    driver::DescribeTransactionsCall,
};

use super::super::{EngineHostError, EngineHostResources};

pub(super) struct AdminDescribeTransactionsProgress {
    pub(super) unsettled: usize,
    pub(super) driver_progress: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    now: Moment,
) -> Result<AdminDescribeTransactionsProgress, EngineHostError> {
    let mut host = match resources.describe_transactions.try_host() {
        Ok(host) => host,
        Err(AdminDescribeTransactionsShardLockError::Contended) => {
            return Ok(AdminDescribeTransactionsProgress::contended());
        }
        Err(AdminDescribeTransactionsShardLockError::Poisoned) => {
            return Err(EngineHostError::AdminDescribeTransactionsLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        resources.describe_transactions.close_locked(&mut host);
    }
    let turn = host
        .turn(now)
        .map_err(EngineHostError::AdminDescribeTransactions)?;
    let driver_progress = match turn {
        AdminDescribeTransactionsTurn::Idle => false,
        AdminDescribeTransactionsTurn::Progress => true,
        AdminDescribeTransactionsTurn::Submit(submission) => {
            let (operation_id, deadline, transactional_id) = submission.into_parts();
            let driver = resources
                .driver
                .as_ref()
                .ok_or(EngineHostError::DriverOwnerMissing)?;
            match DescribeTransactionsCall::submit(driver, &transactional_id, deadline.transport())
            {
                Ok(call) => host
                    .accept_call(operation_id, call)
                    .map_err(EngineHostError::AdminDescribeTransactions)?,
                Err(_rejection) => {
                    host.reject_handoff(operation_id)
                        .map_err(EngineHostError::AdminDescribeTransactions)?;
                }
            }
            true
        }
    };
    Ok(AdminDescribeTransactionsProgress {
        unsettled: host.unsettled(),
        driver_progress,
        next_deadline: host.next_deadline(),
    })
}

impl AdminDescribeTransactionsProgress {
    const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        }
    }
}
