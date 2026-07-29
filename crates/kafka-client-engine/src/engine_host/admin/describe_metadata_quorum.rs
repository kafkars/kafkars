//! Fair host turns for AnyBroker Admin `DescribeMetadataQuorum` work.

use kafka_client_core::{Deadline, Moment};

use crate::{
    admin::{
        DescribeMetadataQuorumShardLockError, DescribeMetadataQuorumShardWake,
        DescribeMetadataQuorumShardWakeError, DescribeMetadataQuorumTurn,
    },
    driver::{DescribeMetadataQuorumCall, ReactorWake},
};

use super::super::{EngineHostError, EngineHostResources};

pub(super) struct DescribeMetadataQuorumProgress {
    pub(super) unsettled: usize,
    pub(super) driver_progress: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    now: Moment,
) -> Result<DescribeMetadataQuorumProgress, EngineHostError> {
    let mut host = match resources.describe_metadata_quorum.try_host() {
        Ok(host) => host,
        Err(DescribeMetadataQuorumShardLockError::Contended) => {
            return Ok(DescribeMetadataQuorumProgress::contended());
        }
        Err(DescribeMetadataQuorumShardLockError::Poisoned) => {
            return Err(EngineHostError::DescribeMetadataQuorumLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        resources.describe_metadata_quorum.close_locked(&mut host);
    }
    let turn = host
        .turn(now)
        .map_err(EngineHostError::DescribeMetadataQuorum)?;
    let driver_progress = match turn {
        DescribeMetadataQuorumTurn::Idle => false,
        DescribeMetadataQuorumTurn::Progress => true,
        DescribeMetadataQuorumTurn::Submit(submission) => {
            let (operation_id, deadline, _result_limit) = submission.into_parts();
            let driver = resources
                .driver
                .as_ref()
                .ok_or(EngineHostError::DriverOwnerMissing)?;
            match DescribeMetadataQuorumCall::submit(driver, deadline.transport()) {
                Ok(call) => host
                    .accept_call(operation_id, call)
                    .map_err(EngineHostError::DescribeMetadataQuorum)?,
                Err(rejection) => {
                    let _rejection = rejection;
                    host.reject_handoff(operation_id)
                        .map_err(EngineHostError::DescribeMetadataQuorum)?;
                }
            }
            true
        }
    };
    Ok(DescribeMetadataQuorumProgress {
        unsettled: host.unsettled(),
        driver_progress,
        next_deadline: host.next_deadline(),
    })
}

impl DescribeMetadataQuorumProgress {
    const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        }
    }
}

impl DescribeMetadataQuorumShardWake for ReactorWake {
    fn wake(&self) -> Result<(), DescribeMetadataQuorumShardWakeError> {
        self.request()
            .map_err(|error| DescribeMetadataQuorumShardWakeError::from_io(error.into_io()))
    }
}
