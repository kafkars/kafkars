//! Fair host turns for one committed metadata-quorum voter addition.

use kafka_client_core::{Deadline, Moment};

use crate::{
    admin::{
        AddRaftVoterShardLockError, AddRaftVoterShardWake, AddRaftVoterShardWakeError,
        AddRaftVoterTurn,
    },
    driver::{AddRaftVoterCall, ReactorWake},
};

use super::super::{EngineHostError, EngineHostResources};

pub(super) struct AddRaftVoterProgress {
    pub(super) unsettled: usize,
    pub(super) driver_progress: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    now: Moment,
) -> Result<AddRaftVoterProgress, EngineHostError> {
    let mut host = match resources.add_raft_voter.try_host() {
        Ok(host) => host,
        Err(AddRaftVoterShardLockError::Contended) => {
            return Ok(AddRaftVoterProgress::contended());
        }
        Err(AddRaftVoterShardLockError::Poisoned) => {
            return Err(EngineHostError::AddRaftVoterLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        resources.add_raft_voter.close_locked(&mut host);
    }
    let turn = host.turn(now).map_err(EngineHostError::AddRaftVoter)?;
    let driver_progress = match turn {
        AddRaftVoterTurn::Idle => false,
        AddRaftVoterTurn::Progress => true,
        AddRaftVoterTurn::Submit(submission) => {
            let (operation_id, deadline, plan, _result_limit) = submission.into_parts();
            let driver = resources
                .driver
                .as_ref()
                .ok_or(EngineHostError::DriverOwnerMissing)?;
            match AddRaftVoterCall::submit(driver, &plan, deadline, now) {
                Ok(call) => host
                    .accept_call(operation_id, call)
                    .map_err(EngineHostError::AddRaftVoter)?,
                Err(_rejection) => host
                    .reject_handoff(operation_id)
                    .map_err(EngineHostError::AddRaftVoter)?,
            }
            true
        }
    };
    Ok(AddRaftVoterProgress {
        unsettled: host.unsettled(),
        driver_progress,
        next_deadline: host.next_deadline(),
    })
}

impl AddRaftVoterProgress {
    const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        }
    }
}

impl AddRaftVoterShardWake for ReactorWake {
    fn wake(&self) -> Result<(), AddRaftVoterShardWakeError> {
        self.request()
            .map_err(|error| AddRaftVoterShardWakeError::from_io(error.into_io()))
    }
}
