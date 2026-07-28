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
    classic_group_partition_count_settlement::ClassicGroupPartitionCountSettlementTurn,
    classic_group_partition_count_submission::ClassicGroupPartitionCountSubmissionTurn,
    classic_group_position::ClassicGroupPositionCloseTurn,
    classic_group_rediscovery_execution::{
        ClassicCoordinatorInvalidationTurn, ClassicCoordinatorInvalidationTurn::Blocked,
    },
    classic_group_rejoin_due::ClassicGroupRejoinDueTurn,
    classic_group_sync_settlement::ClassicGroupSyncSettlementTurn,
    classic_group_sync_submission::ClassicGroupSyncSubmissionTurn,
    registry::GroupConsumerRegistry,
    registry_entry::{GroupConsumerEntry, GroupConsumerEntryState},
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
                    || partition_count_blocked
                    || heartbeat_blocked
                    || sync_blocked =>
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
        let mut driver_owned_close = false;
        for entry in &mut self.entries {
            if entry.state != GroupConsumerEntryState::Closing {
                continue;
            }
            match close_entry_position(entry, now)? {
                ClassicGroupPositionCloseTurn::Progress => {
                    return Ok(GroupConsumerMembershipTurn::Progress);
                }
                ClassicGroupPositionCloseTurn::Blocked => {
                    driver_owned_close = true;
                    continue;
                }
                ClassicGroupPositionCloseTurn::Idle => {}
            }
            if entry.heartbeat.blocks_close() {
                driver_owned_close = true;
                continue;
            }
            let heartbeat_was_local = entry.heartbeat.unsettled() != 0;
            match entry.execution.close_if_local(
                &mut entry.classic,
                &mut entry.catalog,
                &mut entry.processing_lease,
                &mut entry.fetch,
            )? {
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
        for entry in &mut self.entries {
            if entry.state != GroupConsumerEntryState::Active
                || entry.fault.is_some()
                || entry.catalog.live_assignment().is_some()
                || entry.classic.machine().live_assignment().is_some()
                || entry.position.is_dormant()
            {
                continue;
            }
            match close_entry_position(entry, now)? {
                ClassicGroupPositionCloseTurn::Progress => {
                    return Ok(GroupConsumerMembershipTurn::Progress);
                }
                ClassicGroupPositionCloseTurn::Blocked => {
                    driver_owned_close = true;
                }
                ClassicGroupPositionCloseTurn::Idle => {}
            }
        }
        if self.expire_one_prepared_heartbeat(now)? {
            return Ok(GroupConsumerMembershipTurn::Progress);
        }
        for entry in &mut self.entries {
            if entry.fault.is_some() {
                continue;
            }
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

fn close_entry_position(
    entry: &mut GroupConsumerEntry,
    now: Moment,
) -> Result<ClassicGroupPositionCloseTurn, ClassicGroupExecutionError> {
    match entry.position.close_position_if_local(now) {
        Ok(turn) => Ok(turn),
        Err(failure) => {
            let error = failure.error();
            entry.fault = Some(
                super::classic_group_entry_fault::ClassicGroupEntryFault::PositionRejection(
                    failure,
                ),
            );
            Err(ClassicGroupExecutionError::Position(error))
        }
    }
}
