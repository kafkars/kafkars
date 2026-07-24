//! Bounded host turns for concrete any-broker `DescribeTopics` work.

use kafka_client_core::{Deadline, DescribeTopicsInput, Moment};

use crate::admin::{DescribeTopicsShardLockError, DescribeTopicsTurn};

use super::super::{EngineHostError, EngineHostResources};

const DESCRIBE_TOPICS_COMPLETION_BUDGET: usize = 16;

pub(super) struct DescribeTopicsProgress {
    pub(super) unsettled: usize,
    pub(super) driver_progress: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    now: Moment,
) -> Result<DescribeTopicsProgress, EngineHostError> {
    let Some(permit) = resources.describe_topics_calls.try_reserve() else {
        return snapshot(&resources.describe_topics);
    };
    let mut host = match resources.describe_topics.try_host() {
        Ok(host) => host,
        Err(DescribeTopicsShardLockError::Contended) => {
            return Ok(DescribeTopicsProgress::contended());
        }
        Err(DescribeTopicsShardLockError::Poisoned) => {
            return Err(EngineHostError::DescribeTopicsLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        resources.describe_topics.close_locked(&mut host);
    }
    let turn = host.turn(now).map_err(EngineHostError::DescribeTopics)?;
    let driver_progress = match turn {
        DescribeTopicsTurn::Idle => false,
        DescribeTopicsTurn::Progress => true,
        DescribeTopicsTurn::Submit(submission) => {
            let (operation_id, deadline, plan, retained_bytes) = submission.into_parts();
            let driver = resources
                .driver
                .as_ref()
                .ok_or(EngineHostError::DriverOwnerMissing)?;
            match permit.submit(driver, operation_id, deadline, plan, retained_bytes) {
                Ok(()) => host
                    .apply(operation_id, DescribeTopicsInput::DriverAccepted)
                    .map_err(EngineHostError::DescribeTopics)?,
                Err(rejection) => host
                    .apply(operation_id, rejection.into_core_input())
                    .map_err(EngineHostError::DescribeTopics)?,
            }
            true
        }
    };
    Ok(DescribeTopicsProgress {
        unsettled: host.unsettled(),
        driver_progress,
        next_deadline: host.next_deadline(),
    })
}

pub(super) fn apply_completions(
    resources: &mut EngineHostResources,
) -> Result<bool, EngineHostError> {
    let mut host = match resources.describe_topics.try_host() {
        Ok(host) => host,
        Err(DescribeTopicsShardLockError::Contended) => return Ok(false),
        Err(DescribeTopicsShardLockError::Poisoned) => {
            return Err(EngineHostError::DescribeTopicsLockPoisoned);
        }
    };
    let mut progress = false;
    for _attempt in 0..DESCRIBE_TOPICS_COMPLETION_BUDGET {
        let Some(settled) = resources
            .describe_topics_calls
            .poll_next_ready()
            .map_err(EngineHostError::DescribeTopicsCompletion)?
        else {
            break;
        };
        let operation_id = settled.operation_id();
        let input = settled.take_input().ok_or(EngineHostError::DescribeTopics(
            crate::admin::DescribeTopicsHostError::MissingTerminal,
        ))?;
        host.apply(operation_id, input)
            .map_err(EngineHostError::DescribeTopics)?;
        resources.describe_topics_calls.discard_settled();
        progress = true;
    }
    Ok(progress)
}

fn snapshot(
    owner: &crate::admin::DescribeTopicsShardOwner,
) -> Result<DescribeTopicsProgress, EngineHostError> {
    let host = match owner.try_host() {
        Ok(host) => host,
        Err(DescribeTopicsShardLockError::Contended) => {
            return Ok(DescribeTopicsProgress::contended());
        }
        Err(DescribeTopicsShardLockError::Poisoned) => {
            return Err(EngineHostError::DescribeTopicsLockPoisoned);
        }
    };
    Ok(DescribeTopicsProgress {
        unsettled: host.unsettled(),
        driver_progress: false,
        next_deadline: host.next_deadline(),
    })
}

impl DescribeTopicsProgress {
    pub(super) const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        }
    }
}
