//! Fair host turns for one destructive metadata-quorum voter removal.

use kafka_client_core::{Deadline, Moment};

use crate::{
    admin::{
        RemoveRaftVoterShardLockError, RemoveRaftVoterShardWake, RemoveRaftVoterShardWakeError,
        RemoveRaftVoterTurn,
    },
    driver::{ReactorWake, RemoveRaftVoterCall},
};

use super::super::{EngineHostError, EngineHostResources};

pub(super) struct RemoveRaftVoterProgress {
    pub(super) unsettled: usize,
    pub(super) driver_progress: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    now: Moment,
) -> Result<RemoveRaftVoterProgress, EngineHostError> {
    let mut host = match resources.remove_raft_voter.try_host() {
        Ok(host) => host,
        Err(RemoveRaftVoterShardLockError::Contended) => {
            return Ok(RemoveRaftVoterProgress::contended());
        }
        Err(RemoveRaftVoterShardLockError::Poisoned) => {
            return Err(EngineHostError::RemoveRaftVoterLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        resources.remove_raft_voter.close_locked(&mut host);
    }
    let turn = host.turn(now).map_err(EngineHostError::RemoveRaftVoter)?;
    let driver_progress = match turn {
        RemoveRaftVoterTurn::Idle => false,
        RemoveRaftVoterTurn::Progress => true,
        RemoveRaftVoterTurn::Submit(submission) => {
            let (operation_id, deadline, plan, _result_limit) = submission.into_parts();
            let driver = resources
                .driver
                .as_ref()
                .ok_or(EngineHostError::DriverOwnerMissing)?;
            match RemoveRaftVoterCall::submit(driver, &plan, deadline) {
                Ok(call) => host
                    .accept_call(operation_id, call)
                    .map_err(EngineHostError::RemoveRaftVoter)?,
                Err(_rejection) => host
                    .reject_handoff(operation_id)
                    .map_err(EngineHostError::RemoveRaftVoter)?,
            }
            true
        }
    };
    Ok(RemoveRaftVoterProgress {
        unsettled: host.unsettled(),
        driver_progress,
        next_deadline: host.next_deadline(),
    })
}

impl RemoveRaftVoterProgress {
    const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        }
    }
}

impl RemoveRaftVoterShardWake for ReactorWake {
    fn wake(&self) -> Result<(), RemoveRaftVoterShardWakeError> {
        self.request()
            .map_err(|error| RemoveRaftVoterShardWakeError::from_io(error.into_io()))
    }
}
