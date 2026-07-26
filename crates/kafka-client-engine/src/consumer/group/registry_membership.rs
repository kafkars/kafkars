//! One bounded per-entry membership timeout or close transition per registry turn.

use kafka_client_core::Moment;

use crate::driver::DriverOwner;

use super::{
    classic_group_execution::ClassicGroupExecutionError,
    classic_group_execution_close::ClassicGroupCloseProgress,
    classic_group_heartbeat_prepare::ClassicHeartbeatPreparationTurn,
    classic_group_heartbeat_settlement::ClassicHeartbeatSettlementTurn,
    classic_group_heartbeat_submission::ClassicHeartbeatSubmissionTurn,
    classic_group_join_execution::ClassicGroupJoinSubmissionTurn,
    classic_group_join_settlement::ClassicGroupJoinSettlementTurn,
    classic_group_rediscovery_execution::{
        ClassicCoordinatorInvalidationTurn, ClassicCoordinatorInvalidationTurn::Blocked,
    },
    classic_group_rejoin_due::ClassicGroupRejoinDueTurn,
    classic_group_sync_settlement::ClassicGroupSyncSettlementTurn,
    classic_group_sync_submission::ClassicGroupSyncSubmissionTurn,
    registry::GroupConsumerRegistry,
    registry_entry::GroupConsumerEntryState,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum GroupConsumerMembershipTurn {
    Idle,
    Progress,
    Blocked,
}
impl GroupConsumerRegistry {
    pub(super) fn turn_membership(
        &mut self,
        now: Moment,
        clock: &crate::clock::MonotonicClock,
        driver: &DriverOwner,
    ) -> Result<GroupConsumerMembershipTurn, ClassicGroupExecutionError> {
        if self.entries.iter().any(|entry| entry.fault.is_some()) {
            return Err(ClassicGroupExecutionError::EntryFault);
        }
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
        let join_blocked = match self.settle_one_classic_join(now)? {
            ClassicGroupJoinSettlementTurn::Progress => {
                return Ok(GroupConsumerMembershipTurn::Progress);
            }
            ClassicGroupJoinSettlementTurn::Blocked => true,
            ClassicGroupJoinSettlementTurn::Idle => false,
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
        let sync_blocked = match self.submit_one_classic_sync(driver)? {
            ClassicGroupSyncSubmissionTurn::Progress => {
                return Ok(GroupConsumerMembershipTurn::Progress);
            }
            ClassicGroupSyncSubmissionTurn::Blocked => true,
            ClassicGroupSyncSubmissionTurn::Idle => false,
        };
        Ok(match self.submit_one_classic_join(driver)? {
            ClassicGroupJoinSubmissionTurn::Idle
                if rediscovery_blocked || join_blocked || heartbeat_blocked || sync_blocked =>
            {
                GroupConsumerMembershipTurn::Blocked
            }
            ClassicGroupJoinSubmissionTurn::Idle => GroupConsumerMembershipTurn::Idle,
            ClassicGroupJoinSubmissionTurn::Progress => GroupConsumerMembershipTurn::Progress,
            ClassicGroupJoinSubmissionTurn::Blocked => GroupConsumerMembershipTurn::Blocked,
        })
    }
    pub(super) fn turn_local_membership(
        &mut self,
        now: Moment,
    ) -> Result<GroupConsumerMembershipTurn, ClassicGroupExecutionError> {
        if self.entries.iter().any(|entry| entry.fault.is_some()) {
            return Err(ClassicGroupExecutionError::EntryFault);
        }
        let mut driver_owned_close = false;
        for entry in &mut self.entries {
            if entry.state != GroupConsumerEntryState::Closing {
                continue;
            }
            if entry.heartbeat.blocks_close() {
                driver_owned_close = true;
                continue;
            }
            let heartbeat_was_local = entry.heartbeat.unsettled() != 0;
            match entry
                .execution
                .close_if_local(&mut entry.classic, &mut entry.catalog)?
            {
                ClassicGroupCloseProgress::Progress => {
                    entry
                        .heartbeat
                        .clear_local()
                        .map_err(|_error| ClassicGroupExecutionError::HeartbeatState)?;
                    if let Some(schedule) = entry.rejoin.schedule() {
                        entry
                            .rejoin
                            .clear_rejoin_exact(schedule)
                            .map_err(|_error| ClassicGroupExecutionError::RejoinState)?;
                    }
                    return Ok(GroupConsumerMembershipTurn::Progress);
                }
                ClassicGroupCloseProgress::DriverOwned => driver_owned_close = true,
                ClassicGroupCloseProgress::AlreadyClosed if heartbeat_was_local => {
                    entry
                        .heartbeat
                        .clear_local()
                        .map_err(|_error| ClassicGroupExecutionError::HeartbeatState)?;
                    if let Some(schedule) = entry.rejoin.schedule() {
                        entry
                            .rejoin
                            .clear_rejoin_exact(schedule)
                            .map_err(|_error| ClassicGroupExecutionError::RejoinState)?;
                    }
                    return Ok(GroupConsumerMembershipTurn::Progress);
                }
                ClassicGroupCloseProgress::AlreadyClosed => {
                    if let Some(schedule) = entry.rejoin.schedule() {
                        entry
                            .rejoin
                            .clear_rejoin_exact(schedule)
                            .map_err(|_error| ClassicGroupExecutionError::RejoinState)?;
                        return Ok(GroupConsumerMembershipTurn::Progress);
                    }
                }
            }
        }
        if self.expire_one_prepared_heartbeat(now)? {
            return Ok(GroupConsumerMembershipTurn::Progress);
        }
        for entry in &mut self.entries {
            if entry.execution.expire_if_due(&mut entry.classic, now)? {
                return Ok(GroupConsumerMembershipTurn::Progress);
            }
        }
        Ok(if driver_owned_close {
            GroupConsumerMembershipTurn::Blocked
        } else {
            GroupConsumerMembershipTurn::Idle
        })
    }
}
