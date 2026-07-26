//! Concrete one-action position settlement and submission host turn.

use kafka_client_core::Moment;

use crate::driver::DriverOwner;

use super::{
    super::{classic_group_execution::ClassicGroupExecutionError, registry::GroupConsumerRegistry},
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
        if self.settle_one_classic_group_position(now)? == Progress {
            return Ok(GroupConsumerPositionTurn::Progress);
        }
        Ok(match self.submit_one_classic_group_position(driver, now)? {
            ClassicGroupPositionSubmissionTurn::Idle => GroupConsumerPositionTurn::Idle,
            ClassicGroupPositionSubmissionTurn::Progress => GroupConsumerPositionTurn::Progress,
            Blocked => GroupConsumerPositionTurn::Blocked,
        })
    }
}
