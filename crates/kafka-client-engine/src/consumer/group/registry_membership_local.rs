//! Local classic-group close and deadline progress after broker-owned work settles.

use kafka_client_core::Moment;

use super::{
    classic_group_execution::ClassicGroupExecutionError,
    classic_group_execution_close::ClassicGroupCloseProgress,
    classic_group_leave::resolve_local_leave_without_member,
    classic_group_position::{ClassicGroupPositionCloseTurn, close_entry_position},
    registry::GroupConsumerRegistry,
    registry_entry::GroupConsumerEntryState,
    registry_membership::GroupConsumerMembershipTurn,
};

impl GroupConsumerRegistry {
    #[expect(
        clippy::too_many_lines,
        reason = "one deterministic registry turn preserves ordered close and expiry ownership"
    )]
    pub(super) fn turn_local_membership(
        &mut self,
        now: Moment,
    ) -> Result<GroupConsumerMembershipTurn, ClassicGroupExecutionError> {
        let mut driver_owned_close = false;
        for entry in &mut self.entries {
            if entry.uses_consumer_group_protocol() {
                continue;
            }
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
            if entry.uses_consumer_group_protocol() {
                continue;
            }
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
            if entry.uses_consumer_group_protocol() || entry.fault.is_some() {
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
