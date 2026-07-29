//! Fair host turns for one explicit controller-routed broker unregistration.

use kafka_client_core::{Deadline, Moment};

use crate::{
    admin::unregister_broker::{
        UnregisterBrokerHostError, UnregisterBrokerShardLockError, UnregisterBrokerShardWake,
        UnregisterBrokerShardWakeError, UnregisterBrokerTurn,
    },
    driver::{ReactorWake, UnregisterBrokerCall},
};

use super::super::{EngineHostError, EngineHostResources};

pub(super) struct UnregisterBrokerProgress {
    pub(super) unsettled: usize,
    pub(super) driver_progress: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    now: Moment,
) -> Result<UnregisterBrokerProgress, EngineHostError> {
    let mut host = match resources.unregister_broker.try_host() {
        Ok(host) => host,
        Err(UnregisterBrokerShardLockError::Contended) => {
            return Ok(UnregisterBrokerProgress::contended());
        }
        Err(UnregisterBrokerShardLockError::Poisoned) => {
            return Err(EngineHostError::UnregisterBrokerLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        resources.unregister_broker.close_locked(&mut host);
    }
    let turn = match host.turn(now, resources.driver.as_ref()) {
        Ok(turn) => turn,
        Err(UnregisterBrokerHostError::DriverMissing) => {
            return Err(EngineHostError::DriverOwnerMissing);
        }
        Err(error) => return Err(EngineHostError::UnregisterBroker(error)),
    };
    let driver_progress = match turn {
        UnregisterBrokerTurn::Idle => false,
        UnregisterBrokerTurn::Progress => true,
        UnregisterBrokerTurn::Submit(submission) => {
            let (operation_id, deadline, plan, _result_limit) = submission.into_parts();
            let driver = resources
                .driver
                .as_ref()
                .ok_or(EngineHostError::DriverOwnerMissing)?;
            match UnregisterBrokerCall::submit(driver, plan, deadline.transport()) {
                Ok(call) => host
                    .accept_call(operation_id, call)
                    .map_err(EngineHostError::UnregisterBroker)?,
                Err(_rejection) => host
                    .reject_handoff(operation_id)
                    .map_err(EngineHostError::UnregisterBroker)?,
            }
            true
        }
    };
    Ok(UnregisterBrokerProgress {
        unsettled: host.unsettled(),
        driver_progress,
        next_deadline: host.next_deadline(),
    })
}

impl UnregisterBrokerProgress {
    const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        }
    }
}

impl UnregisterBrokerShardWake for ReactorWake {
    fn wake(&self) -> Result<(), UnregisterBrokerShardWakeError> {
        self.request()
            .map_err(|error| UnregisterBrokerShardWakeError::from_io(error.into_io()))
    }
}
