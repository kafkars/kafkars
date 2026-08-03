//! One bounded per-entry membership timeout or close transition per registry turn.

use kafka_client_core::Moment;

use crate::driver::DriverOwner;

use super::{
    classic_group_execution::ClassicGroupExecutionError,
    classic_group_heartbeat_prepare::ClassicHeartbeatPreparationTurn,
    classic_group_heartbeat_settlement::ClassicHeartbeatSettlementTurn,
    classic_group_heartbeat_submission::ClassicHeartbeatSubmissionTurn,
    classic_group_join_execution::ClassicGroupJoinSubmissionTurn,
    classic_group_join_settlement::ClassicGroupJoinSettlementTurn,
    classic_group_leave::ClassicGroupLeaveTurn,
    classic_group_partition_count_settlement::ClassicGroupPartitionCountSettlementTurn,
    classic_group_partition_count_submission::ClassicGroupPartitionCountSubmissionTurn,
    classic_group_reconciliation_loss::ClassicGroupReconciliationLossTurn,
    classic_group_reconciliation_turn::ClassicGroupReconciliationTurn,
    classic_group_rediscovery_execution::{
        ClassicCoordinatorInvalidationTurn, ClassicCoordinatorInvalidationTurn::Blocked,
    },
    classic_group_rejoin_due::ClassicGroupRejoinDueTurn,
    classic_group_sync_settlement::ClassicGroupSyncSettlementTurn,
    classic_group_sync_submission::ClassicGroupSyncSubmissionTurn,
    consumer_group_assignment_retirement::ConsumerGroupAssignmentRetirementTurn,
    consumer_group_close::ConsumerGroupCloseTurn,
    consumer_group_heartbeat_due::ConsumerGroupHeartbeatDueTurn,
    consumer_group_heartbeat_settlement::ConsumerGroupHeartbeatSettlementTurn,
    consumer_group_heartbeat_submission::ConsumerGroupHeartbeatSubmissionTurn,
    consumer_group_topic_identity_turn::ConsumerGroupTopicIdentityTurn,
    registry::GroupConsumerRegistry,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum GroupConsumerMembershipTurn {
    Idle,
    Progress,
    Blocked,
}
impl GroupConsumerRegistry {
    #[expect(
        clippy::too_many_lines,
        reason = "one fair membership scheduler turn must preserve the explicit domain order"
    )]
    pub(super) fn turn_membership(
        &mut self,
        now: Moment,
        clock: &crate::clock::MonotonicClock,
        driver: &DriverOwner,
    ) -> Result<GroupConsumerMembershipTurn, ClassicGroupExecutionError> {
        let consumer_heartbeat_blocked = match self
            .settle_one_consumer_group_heartbeat(now, clock)
            .map_err(|_error| ClassicGroupExecutionError::ConsumerGroup)?
        {
            ConsumerGroupHeartbeatSettlementTurn::Progress => {
                return Ok(GroupConsumerMembershipTurn::Progress);
            }
            ConsumerGroupHeartbeatSettlementTurn::Blocked => true,
            ConsumerGroupHeartbeatSettlementTurn::Idle => false,
        };
        let consumer_assignment_blocked =
            match self.turn_one_consumer_group_assignment_retirement(now, clock)? {
                ConsumerGroupAssignmentRetirementTurn::Progress => {
                    return Ok(GroupConsumerMembershipTurn::Progress);
                }
                ConsumerGroupAssignmentRetirementTurn::Blocked => true,
                ConsumerGroupAssignmentRetirementTurn::Idle => false,
            };
        let consumer_topic_blocked = match self
            .turn_one_consumer_group_topic_identity(now, driver)
            .map_err(|_error| ClassicGroupExecutionError::ConsumerGroup)?
        {
            ConsumerGroupTopicIdentityTurn::Progress => {
                return Ok(GroupConsumerMembershipTurn::Progress);
            }
            ConsumerGroupTopicIdentityTurn::Blocked => true,
            ConsumerGroupTopicIdentityTurn::Idle => false,
        };
        if self
            .prepare_one_consumer_group_load_retry(now)
            .map_err(|_error| ClassicGroupExecutionError::ConsumerGroup)?
            == ConsumerGroupHeartbeatDueTurn::Progress
        {
            return Ok(GroupConsumerMembershipTurn::Progress);
        }
        let consumer_close_blocked = match self
            .turn_one_consumer_group_close(now)
            .map_err(|_error| ClassicGroupExecutionError::ConsumerGroup)?
        {
            ConsumerGroupCloseTurn::Progress => {
                return Ok(GroupConsumerMembershipTurn::Progress);
            }
            ConsumerGroupCloseTurn::Blocked => true,
            ConsumerGroupCloseTurn::Idle => false,
        };
        if self
            .prepare_one_consumer_group_heartbeat(now, clock)
            .map_err(|_error| ClassicGroupExecutionError::ConsumerGroup)?
            == ConsumerGroupHeartbeatDueTurn::Progress
        {
            return Ok(GroupConsumerMembershipTurn::Progress);
        }
        let consumer_submission_blocked = match self
            .submit_one_consumer_group_heartbeat(now, driver)
            .map_err(|_error| ClassicGroupExecutionError::ConsumerGroup)?
        {
            ConsumerGroupHeartbeatSubmissionTurn::Progress => {
                return Ok(GroupConsumerMembershipTurn::Progress);
            }
            ConsumerGroupHeartbeatSubmissionTurn::Blocked => true,
            ConsumerGroupHeartbeatSubmissionTurn::Idle => false,
        };
        let rediscovery_blocked = match self.drive_one_classic_coordinator_invalidation(driver)? {
            ClassicCoordinatorInvalidationTurn::Progress => {
                return Ok(GroupConsumerMembershipTurn::Progress);
            }
            Blocked => true,
            ClassicCoordinatorInvalidationTurn::Idle => false,
        };
        if self.settle_one_classic_heartbeat(now)? == ClassicHeartbeatSettlementTurn::Progress {
            return Ok(GroupConsumerMembershipTurn::Progress);
        }
        if self.settle_one_classic_sync(now)? == ClassicGroupSyncSettlementTurn::Progress {
            return Ok(GroupConsumerMembershipTurn::Progress);
        }
        let reconciliation_loss_blocked =
            match self.turn_one_classic_group_reconciliation_loss(now)? {
                ClassicGroupReconciliationLossTurn::Progress => {
                    return Ok(GroupConsumerMembershipTurn::Progress);
                }
                ClassicGroupReconciliationLossTurn::Blocked => true,
                ClassicGroupReconciliationLossTurn::Idle => false,
            };
        let reconciliation_finish_blocked =
            match self.finish_one_classic_group_reconciliation(now, clock)? {
                ClassicGroupReconciliationTurn::Progress => {
                    return Ok(GroupConsumerMembershipTurn::Progress);
                }
                ClassicGroupReconciliationTurn::Blocked => true,
                ClassicGroupReconciliationTurn::Idle => false,
            };
        let reconciliation_stage_blocked = match self.stage_one_classic_group_reconciliation(now)? {
            ClassicGroupReconciliationTurn::Progress => {
                return Ok(GroupConsumerMembershipTurn::Progress);
            }
            ClassicGroupReconciliationTurn::Blocked => true,
            ClassicGroupReconciliationTurn::Idle => false,
        };
        match self.settle_one_classic_join(now)? {
            ClassicGroupJoinSettlementTurn::Progress => {
                return Ok(GroupConsumerMembershipTurn::Progress);
            }
            ClassicGroupJoinSettlementTurn::Idle => {}
        }
        if self.settle_one_classic_partition_count(now)?
            == ClassicGroupPartitionCountSettlementTurn::Progress
        {
            return Ok(GroupConsumerMembershipTurn::Progress);
        }
        let leave_blocked = match self.turn_one_classic_group_leave(now, driver) {
            ClassicGroupLeaveTurn::Progress => {
                return Ok(GroupConsumerMembershipTurn::Progress);
            }
            ClassicGroupLeaveTurn::Blocked => true,
            ClassicGroupLeaveTurn::Idle => false,
        };
        let local = self.turn_local_membership(now)?;
        if local != GroupConsumerMembershipTurn::Idle {
            return Ok(local);
        }
        if self.prepare_one_classic_rejoin(now, clock)? == ClassicGroupRejoinDueTurn::Progress {
            return Ok(GroupConsumerMembershipTurn::Progress);
        }
        if self.prepare_one_classic_heartbeat(now, clock)?
            == ClassicHeartbeatPreparationTurn::Progress
        {
            return Ok(GroupConsumerMembershipTurn::Progress);
        }
        let heartbeat_blocked = match self.submit_one_classic_heartbeat(driver)? {
            ClassicHeartbeatSubmissionTurn::Progress => {
                return Ok(GroupConsumerMembershipTurn::Progress);
            }
            ClassicHeartbeatSubmissionTurn::Blocked => true,
            ClassicHeartbeatSubmissionTurn::Idle => false,
        };
        let partition_count_blocked = match self.submit_one_classic_partition_count(driver)? {
            ClassicGroupPartitionCountSubmissionTurn::Progress => {
                return Ok(GroupConsumerMembershipTurn::Progress);
            }
            ClassicGroupPartitionCountSubmissionTurn::Blocked => true,
            ClassicGroupPartitionCountSubmissionTurn::Idle => false,
        };
        let sync_blocked = match self.submit_one_classic_sync(driver)? {
            ClassicGroupSyncSubmissionTurn::Progress => {
                return Ok(GroupConsumerMembershipTurn::Progress);
            }
            ClassicGroupSyncSubmissionTurn::Blocked => true,
            ClassicGroupSyncSubmissionTurn::Idle => false,
        };
        Ok(match self.submit_one_classic_join(driver)? {
            ClassicGroupJoinSubmissionTurn::Idle
                if rediscovery_blocked
                    || consumer_assignment_blocked
                    || consumer_heartbeat_blocked
                    || consumer_submission_blocked
                    || consumer_topic_blocked
                    || consumer_close_blocked
                    || leave_blocked
                    || partition_count_blocked
                    || heartbeat_blocked
                    || sync_blocked
                    || reconciliation_loss_blocked
                    || reconciliation_finish_blocked
                    || reconciliation_stage_blocked =>
            {
                GroupConsumerMembershipTurn::Blocked
            }
            ClassicGroupJoinSubmissionTurn::Idle => GroupConsumerMembershipTurn::Idle,
            ClassicGroupJoinSubmissionTurn::Progress => GroupConsumerMembershipTurn::Progress,
            ClassicGroupJoinSubmissionTurn::Blocked => GroupConsumerMembershipTurn::Blocked,
        })
    }
}
