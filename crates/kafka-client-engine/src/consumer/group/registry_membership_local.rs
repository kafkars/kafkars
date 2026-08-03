//! Local classic-group close and deadline progress after broker-owned work settles.

use kafka_client_core::Moment;

use super::{
    classic_group_execution::ClassicGroupExecutionError,
    classic_group_execution_close::ClassicGroupCloseProgress,
    classic_group_leave::resolve_local_leave_without_member,
    classic_group_position::{
        ClassicGroupPositionCloseTurn, ClassicGroupPositionExecutionState,
        ClassicGroupPositionPreparation, close_entry_position,
    },
    registry::GroupConsumerRegistry,
    registry_entry::{GroupConsumerEntry, GroupConsumerEntryState},
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
            if transfer_reconciliation_position_for_close(entry)? {
                return Ok(GroupConsumerMembershipTurn::Progress);
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
            let close = if entry.classic_reconciliation.is_some() {
                entry.execution.close_reconciliation_if_local(
                    &mut entry.classic,
                    &mut entry.catalog,
                    &mut entry.processing_lease,
                    &mut entry.fetch,
                    entry.rejoin.schedule(),
                    &mut entry.classic_reconciliation,
                )?
            } else {
                entry.execution.close_if_local(
                    &mut entry.classic,
                    &mut entry.catalog,
                    &mut entry.processing_lease,
                    &mut entry.fetch,
                )?
            };
            match close {
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

/// Transfers an unsubmitted added-position owner into the ordinary local-close
/// state machine. The next bounded turn rejects or consumes it through the
/// same deterministic position seam used by every other close.
fn transfer_reconciliation_position_for_close(
    entry: &mut GroupConsumerEntry,
) -> Result<bool, ClassicGroupExecutionError> {
    let Some(pending) = entry.classic_reconciliation.as_mut() else {
        return Ok(false);
    };
    if pending.position_was_installed() {
        return Ok(false);
    }
    if !entry.position.is_dormant() {
        return Err(ClassicGroupExecutionError::PositionPending);
    }
    let position = pending
        .take_position()
        .ok_or(ClassicGroupExecutionError::PositionPending)?;
    entry.position.set(match position {
        ClassicGroupPositionPreparation::Prepared(prepared) => {
            ClassicGroupPositionExecutionState::Prepared(prepared)
        }
        ClassicGroupPositionPreparation::Complete(completed) => {
            ClassicGroupPositionExecutionState::Complete(completed)
        }
    });
    Ok(true)
}
