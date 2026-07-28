//! Concrete one-action position settlement and submission host turn.

use kafka_client_core::Moment;

use crate::driver::DriverOwner;

use super::super::classic_group_position_reset::ClassicGroupPositionResetTurn;
use super::{
    super::{
        classic_group_entry_fault::ClassicGroupEntryFault,
        classic_group_execution::ClassicGroupExecutionError, registry::GroupConsumerRegistry,
    },
    registry_settlement::ClassicGroupPositionSettlementTurn::Progress,
    registry_submission::{
        ClassicGroupPositionSubmissionTurn, ClassicGroupPositionSubmissionTurn::Blocked,
    },
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer::group) enum GroupConsumerPositionTurn {
    Idle,
    Progress,
    Blocked,
}

impl GroupConsumerRegistry {
    pub(in crate::consumer::group) fn turn_position(
        &mut self,
        now: Moment,
        driver: &DriverOwner,
    ) -> Result<GroupConsumerPositionTurn, ClassicGroupExecutionError> {
        if self.terminalize_one_classic_group_position_failure() {
            return Ok(GroupConsumerPositionTurn::Progress);
        }
        if self.settle_one_classic_group_position(now)? == Progress {
            return Ok(GroupConsumerPositionTurn::Progress);
        }
        if self
            .settle_one_classic_group_position_reset(now)
            .map_err(ClassicGroupExecutionError::Position)?
            == ClassicGroupPositionResetTurn::Progress
        {
            return Ok(GroupConsumerPositionTurn::Progress);
        }
        if self
            .begin_one_classic_group_position_reset(now)
            .map_err(ClassicGroupExecutionError::Position)?
            == ClassicGroupPositionResetTurn::Progress
        {
            return Ok(GroupConsumerPositionTurn::Progress);
        }
        if self
            .submit_one_classic_group_position_reset(driver, now)
            .map_err(ClassicGroupExecutionError::Position)?
            == ClassicGroupPositionResetTurn::Progress
        {
            return Ok(GroupConsumerPositionTurn::Progress);
        }
        Ok(match self.submit_one_classic_group_position(driver, now)? {
            ClassicGroupPositionSubmissionTurn::Idle => GroupConsumerPositionTurn::Idle,
            ClassicGroupPositionSubmissionTurn::Progress => GroupConsumerPositionTurn::Progress,
            Blocked => GroupConsumerPositionTurn::Blocked,
        })
    }

    /// Moves one semantic terminal into the exact entry fault observed by recv.
    pub(in crate::consumer::group) fn terminalize_one_classic_group_position_failure(
        &mut self,
    ) -> bool {
        for entry in &mut self.entries {
            if entry.fault.is_some() {
                continue;
            }
            let Some(failure) = entry.position.take_failure() else {
                continue;
            };
            entry.retain_position_failure_observation(failure.observation_kind());
            entry.fault = Some(ClassicGroupEntryFault::PositionFailure(failure));
            return true;
        }
        false
    }
}
