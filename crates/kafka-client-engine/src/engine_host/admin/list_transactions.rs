//! Fair host turns for discovery followed by exact-broker transaction listing.

use kafka_client_core::{Deadline, Moment};

use crate::{
    admin::{
        AdminListTransactionsShardLockError, AdminListTransactionsShardWake,
        AdminListTransactionsShardWakeError, AdminListTransactionsSubmissionKind,
        AdminListTransactionsTurn,
    },
    driver::{ListTransactionsCall, ReactorWake},
    protocol::admin::list_transactions::{ListTransactionsRequestPlan, list_transactions_request},
};

use super::super::{EngineHostError, EngineHostResources};

pub(super) struct AdminListTransactionsProgress {
    pub(super) unsettled: usize,
    pub(super) driver_progress: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    now: Moment,
) -> Result<AdminListTransactionsProgress, EngineHostError> {
    let mut host = match resources.list_transactions.try_host() {
        Ok(host) => host,
        Err(AdminListTransactionsShardLockError::Contended) => {
            return Ok(AdminListTransactionsProgress::contended());
        }
        Err(AdminListTransactionsShardLockError::Poisoned) => {
            return Err(EngineHostError::AdminListTransactionsLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        resources.list_transactions.close_locked(&mut host);
    }
    let turn = host
        .turn(now)
        .map_err(EngineHostError::AdminListTransactions)?;
    let driver_progress = match turn {
        AdminListTransactionsTurn::Idle => false,
        AdminListTransactionsTurn::Progress => true,
        AdminListTransactionsTurn::Submit(submission) => {
            let (operation_id, deadline, kind) = submission.into_parts();
            let call = match kind {
                AdminListTransactionsSubmissionKind::Discovery => {
                    ListTransactionsCall::submit_discovery(
                        resources
                            .driver
                            .as_ref()
                            .ok_or(EngineHostError::DriverOwnerMissing)?,
                        deadline.transport(),
                    )
                }
                AdminListTransactionsSubmissionKind::Broker {
                    broker_id,
                    plan,
                    retained_limit,
                } => {
                    let protocol_plan = ListTransactionsRequestPlan::new(
                        plan.state_filters(),
                        plan.producer_id_filters(),
                        plan.duration_filter_ms(),
                        plan.transactional_id_pattern(),
                    );
                    let Ok((request, minimum_version)) =
                        list_transactions_request(protocol_plan, retained_limit)
                    else {
                        host.reject_handoff(operation_id)
                            .map_err(EngineHostError::AdminListTransactions)?;
                        return Ok(AdminListTransactionsProgress {
                            unsettled: host.unsettled(),
                            driver_progress: true,
                            next_deadline: host.next_deadline(),
                        });
                    };
                    let driver = resources
                        .driver
                        .as_ref()
                        .ok_or(EngineHostError::DriverOwnerMissing)?;
                    ListTransactionsCall::submit_broker(
                        driver,
                        broker_id,
                        request,
                        minimum_version,
                        deadline.transport(),
                    )
                }
            };
            match call {
                Ok(call) => host
                    .accept_call(operation_id, call)
                    .map_err(EngineHostError::AdminListTransactions)?,
                Err(rejection) => {
                    drop(rejection.into_source());
                    host.reject_handoff(operation_id)
                        .map_err(EngineHostError::AdminListTransactions)?;
                }
            }
            true
        }
    };
    Ok(AdminListTransactionsProgress {
        unsettled: host.unsettled(),
        driver_progress,
        next_deadline: host.next_deadline(),
    })
}

impl AdminListTransactionsProgress {
    const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        }
    }
}

impl AdminListTransactionsShardWake for ReactorWake {
    fn wake(&self) -> Result<(), AdminListTransactionsShardWakeError> {
        self.request()
            .map_err(|error| AdminListTransactionsShardWakeError::from_io(error.into_io()))
    }
}
