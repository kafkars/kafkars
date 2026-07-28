//! Ordered local membership close and expiry transitions.

use kafka_client_core::Moment;

use super::super::{
    classic_group_execution::ClassicGroupExecutionError,
    classic_group_execution_close::ClassicGroupCloseProgress,
    classic_group_leave::resolve_local_leave_without_member,
    classic_group_position::ClassicGroupPositionCloseTurn,
    registry::GroupConsumerRegistry,
    registry_entry::{GroupConsumerEntry, GroupConsumerEntryState},
};
use super::GroupConsumerMembershipTurn;

impl GroupConsumerRegistry {
    pub(in crate::consumer::group) fn turn_local_membership(
        &mut self,
        now: Moment,
    ) -> Result<GroupConsumerMembershipTurn, ClassicGroupExecutionError> {
        let mut driver_owned_close = false;
        for entry in &mut self.entries {
            if entry.state != GroupConsumerEntryState::Closing {
                continue;
            }
            if !entry.revocation.is_dormant() {
                driver_owned_close = true;
                continue;
            }
            if !entry.leave.allows_local_close() {
                if resolve_local_leave_without_member(entry) {
                    return Ok(GroupConsumerMembershipTurn::Progress);
                }
                driver_owned_close = true;
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
                    clear_local_schedules(entry)?;
                    return Ok(GroupConsumerMembershipTurn::Progress);
                }
                ClassicGroupCloseProgress::DriverOwned => driver_owned_close = true,
                ClassicGroupCloseProgress::AlreadyClosed if heartbeat_was_local => {
                    clear_local_schedules(entry)?;
                    return Ok(GroupConsumerMembershipTurn::Progress);
                }
                ClassicGroupCloseProgress::AlreadyClosed => {
                    if clear_rejoin(entry)? {
                        return Ok(GroupConsumerMembershipTurn::Progress);
                    }
                }
            }
        }
        self.turn_active_position_close(now, driver_owned_close)
    }

    fn turn_active_position_close(
        &mut self,
        now: Moment,
        mut driver_owned_close: bool,
    ) -> Result<GroupConsumerMembershipTurn, ClassicGroupExecutionError> {
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
                ClassicGroupPositionCloseTurn::Blocked => driver_owned_close = true,
                ClassicGroupPositionCloseTurn::Idle => {}
            }
        }
        if self.expire_one_prepared_heartbeat(now)? {
            return Ok(GroupConsumerMembershipTurn::Progress);
        }
        for entry in &mut self.entries {
            if entry.fault.is_none() && entry.execution.expire_if_due(&mut entry.classic, now)? {
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

fn clear_local_schedules(entry: &mut GroupConsumerEntry) -> Result<(), ClassicGroupExecutionError> {
    entry
        .heartbeat
        .clear_local()
        .map_err(|_error| ClassicGroupExecutionError::HeartbeatState)?;
    let _cleared = clear_rejoin(entry)?;
    Ok(())
}

fn clear_rejoin(entry: &mut GroupConsumerEntry) -> Result<bool, ClassicGroupExecutionError> {
    let Some(schedule) = entry.rejoin.schedule() else {
        return Ok(false);
    };
    entry
        .rejoin
        .clear_rejoin_exact(schedule)
        .map_err(|_error| ClassicGroupExecutionError::RejoinState)?;
    Ok(true)
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
                super::super::classic_group_entry_fault::ClassicGroupEntryFault::PositionRejection(
                    failure,
                ),
            );
            Err(ClassicGroupExecutionError::Position(error))
        }
    }
}
