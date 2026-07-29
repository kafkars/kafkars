//! Bounded host turns for concrete controller-routed `DeleteTopics` work.

use kafka_client_core::{Deadline, DeleteTopicsInput, Moment};

use crate::{
    admin::{DeleteTopicsShardLockError, DeleteTopicsTurn},
    driver::DeleteTopicsControllerRefreshPoll,
};

use super::super::{EngineHostError, EngineHostResources};

const DELETE_TOPICS_COMPLETION_BUDGET: usize = 16;

pub(super) struct DeleteTopicsProgress {
    pub(super) unsettled: usize,
    pub(super) driver_progress: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    now: Moment,
) -> Result<DeleteTopicsProgress, EngineHostError> {
    let Some(permit) = resources.delete_topics_calls.try_reserve() else {
        return snapshot(&resources.delete_topics);
    };
    let mut host = match resources.delete_topics.try_host() {
        Ok(host) => host,
        Err(DeleteTopicsShardLockError::Contended) => {
            return Ok(DeleteTopicsProgress::contended());
        }
        Err(DeleteTopicsShardLockError::Poisoned) => {
            return Err(EngineHostError::DeleteTopicsLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        resources.delete_topics.close_locked(&mut host);
    }
    let turn = host.turn(now).map_err(EngineHostError::DeleteTopics)?;
    let driver_progress = match turn {
        DeleteTopicsTurn::Idle => false,
        DeleteTopicsTurn::Progress => true,
        DeleteTopicsTurn::Submit(submission) => {
            let (operation_id, deadline, plan, retained_bytes) = submission.into_parts();
            let driver = resources
                .driver
                .as_ref()
                .ok_or(EngineHostError::DriverOwnerMissing)?;
            match permit.submit(driver, operation_id, deadline, plan, retained_bytes, now) {
                Ok(()) => host
                    .apply(operation_id, DeleteTopicsInput::DriverAccepted)
                    .map_err(EngineHostError::DeleteTopics)?,
                Err(rejection) => host
                    .apply(operation_id, rejection.core_input())
                    .map_err(EngineHostError::DeleteTopics)?,
            }
            true
        }
    };
    Ok(DeleteTopicsProgress {
        unsettled: host.unsettled(),
        driver_progress,
        next_deadline: host.next_deadline(),
    })
}

pub(super) fn apply_completions(
    resources: &mut EngineHostResources,
) -> Result<bool, EngineHostError> {
    let mut host = match resources.delete_topics.try_host() {
        Ok(host) => host,
        Err(DeleteTopicsShardLockError::Contended) => return Ok(false),
        Err(DeleteTopicsShardLockError::Poisoned) => {
            return Err(EngineHostError::DeleteTopicsLockPoisoned);
        }
    };
    let mut progress = false;
    for _attempt in 0..DELETE_TOPICS_COMPLETION_BUDGET {
        let Some(settled) = resources
            .delete_topics_calls
            .poll_next_ready()
            .map_err(EngineHostError::DeleteTopicsCompletion)?
        else {
            break;
        };
        match settled.poll_controller_refresh(resources.driver.as_ref()) {
            DeleteTopicsControllerRefreshPoll::Ready => {}
            DeleteTopicsControllerRefreshPoll::Pending => break,
            DeleteTopicsControllerRefreshPoll::DriverMissing => {
                return Err(EngineHostError::DriverOwnerMissing);
            }
        }
        let operation_id = settled.operation_id();
        let input = settled.take_input().ok_or(EngineHostError::DeleteTopics(
            crate::admin::DeleteTopicsHostError::MissingTerminal,
        ))?;
        host.apply(operation_id, input)
            .map_err(EngineHostError::DeleteTopics)?;
        resources.delete_topics_calls.discard_settled();
        progress = true;
    }
    Ok(progress)
}

fn snapshot(
    owner: &crate::admin::DeleteTopicsShardOwner,
) -> Result<DeleteTopicsProgress, EngineHostError> {
    let host = match owner.try_host() {
        Ok(host) => host,
        Err(DeleteTopicsShardLockError::Contended) => {
            return Ok(DeleteTopicsProgress::contended());
        }
        Err(DeleteTopicsShardLockError::Poisoned) => {
            return Err(EngineHostError::DeleteTopicsLockPoisoned);
        }
    };
    Ok(DeleteTopicsProgress {
        unsettled: host.unsettled(),
        driver_progress: false,
        next_deadline: host.next_deadline(),
    })
}

impl DeleteTopicsProgress {
    const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        }
    }
}
