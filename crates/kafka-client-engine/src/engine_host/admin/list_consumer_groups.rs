//! Fair host turns for discovery followed by exact-broker group listing.

use kafka_client_core::{Deadline, Moment};

use crate::{
    admin::{
        ListConsumerGroupsShardLockError, ListConsumerGroupsShardWake,
        ListConsumerGroupsShardWakeError, ListConsumerGroupsSubmissionKind, ListConsumerGroupsTurn,
    },
    driver::{ListConsumerGroupsCall, ReactorWake},
};

use super::super::{EngineHostError, EngineHostResources};

pub(super) struct ListConsumerGroupsProgress {
    pub(super) unsettled: usize,
    pub(super) driver_progress: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    now: Moment,
) -> Result<ListConsumerGroupsProgress, EngineHostError> {
    let mut host = match resources.list_consumer_groups.try_host() {
        Ok(host) => host,
        Err(ListConsumerGroupsShardLockError::Contended) => {
            return Ok(ListConsumerGroupsProgress::contended());
        }
        Err(ListConsumerGroupsShardLockError::Poisoned) => {
            return Err(EngineHostError::ListConsumerGroupsLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        resources.list_consumer_groups.close_locked(&mut host);
    }
    let turn = host
        .turn(now)
        .map_err(EngineHostError::ListConsumerGroups)?;
    let driver_progress = match turn {
        ListConsumerGroupsTurn::Idle => false,
        ListConsumerGroupsTurn::Progress => true,
        ListConsumerGroupsTurn::Submit(submission) => {
            let (operation_id, deadline, kind) = submission.into_parts();
            let driver = resources
                .driver
                .as_ref()
                .ok_or(EngineHostError::DriverOwnerMissing)?;
            let call = match kind {
                ListConsumerGroupsSubmissionKind::Discovery => {
                    ListConsumerGroupsCall::submit_discovery(driver, deadline.transport())
                }
                ListConsumerGroupsSubmissionKind::Broker { broker_id } => {
                    ListConsumerGroupsCall::submit_broker(driver, broker_id, deadline.transport())
                }
            };
            match call {
                Ok(call) => host
                    .accept_call(operation_id, call)
                    .map_err(EngineHostError::ListConsumerGroups)?,
                Err(rejection) => {
                    drop(rejection.into_source());
                    host.reject_handoff(operation_id)
                        .map_err(EngineHostError::ListConsumerGroups)?;
                }
            }
            true
        }
    };
    Ok(ListConsumerGroupsProgress {
        unsettled: host.unsettled(),
        driver_progress,
        next_deadline: host.next_deadline(),
    })
}

impl ListConsumerGroupsProgress {
    const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        }
    }
}

impl ListConsumerGroupsShardWake for ReactorWake {
    fn wake(&self) -> Result<(), ListConsumerGroupsShardWakeError> {
        self.request()
            .map_err(|error| ListConsumerGroupsShardWakeError::from_io(error.into_io()))
    }
}
