//! Explicit KIP-848 leave preparation and local terminal close ownership.

use kafka_client_core::{
    ConsumerGroupHeartbeatFailure, ConsumerGroupHeartbeatPhase, ConsumerGroupHeartbeatRequestKind,
    Moment,
};

use super::{
    classic_group_leave::{
        GroupConsumerCloseTerminal, GroupConsumerCloseTerminalFailure,
        GroupConsumerCloseTerminalFailureKind,
    },
    consumer_group_assignment_retirement::stage_consumer_group_revocation,
    consumer_group_execution::ConsumerGroupExecutionError,
    registry::GroupConsumerRegistry,
    registry_entry::{GroupConsumerEntry, GroupConsumerEntryState},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ConsumerGroupCloseTurn {
    Idle,
    Progress,
    Blocked,
}

impl GroupConsumerRegistry {
    pub(super) fn turn_one_consumer_group_close(
        &mut self,
        now: Moment,
    ) -> Result<ConsumerGroupCloseTurn, ConsumerGroupExecutionError> {
        let mut blocked = false;
        for entry in &mut self.entries {
            if !entry.uses_consumer_group_protocol()
                || entry.state != GroupConsumerEntryState::Closing
            {
                continue;
            }
            let execution = entry
                .consumer
                .as_ref()
                .ok_or(ConsumerGroupExecutionError::EffectShape)?;
            if execution.heartbeat_call().is_some() || execution.topic_identity_call().is_some() {
                blocked = true;
                continue;
            }
            if execution.machine().phase() == ConsumerGroupHeartbeatPhase::Closed {
                if entry.leave.pending_deadline().is_some()
                    && entry
                        .leave
                        .resolve_consumer_group(GroupConsumerCloseTerminal::Succeeded)
                {
                    return Ok(ConsumerGroupCloseTurn::Progress);
                }
                continue;
            }
            if let Some(prepared) = execution.prepared() {
                if prepared.kind() == ConsumerGroupHeartbeatRequestKind::Leave {
                    if prepared.deadline().core().is_elapsed_at(now) {
                        fail_consumer_group_leave(
                            entry,
                            ConsumerGroupHeartbeatFailure::DeadlineElapsed,
                            deadline_terminal(),
                        )?;
                        return Ok(ConsumerGroupCloseTurn::Progress);
                    }
                    continue;
                }
                let terminal = close_terminal(entry, now);
                close_consumer_group_locally(entry, terminal)?;
                return Ok(ConsumerGroupCloseTurn::Progress);
            }
            let Some(deadline) = entry.leave.pending_deadline() else {
                close_consumer_group_locally(entry, GroupConsumerCloseTerminal::Succeeded)?;
                return Ok(ConsumerGroupCloseTurn::Progress);
            };
            if deadline.core().is_elapsed_at(now) {
                close_consumer_group_locally(entry, deadline_terminal())?;
                return Ok(ConsumerGroupCloseTurn::Progress);
            }
            let prepared = entry
                .consumer
                .as_mut()
                .ok_or(ConsumerGroupExecutionError::EffectShape)?
                .prepare_leave(now, deadline)?;
            if !prepared {
                return Err(ConsumerGroupExecutionError::EffectShape);
            }
            if entry.consumer.as_ref().is_some_and(|execution| {
                execution.machine().phase() == ConsumerGroupHeartbeatPhase::Closed
            }) {
                let resolved = entry
                    .leave
                    .resolve_consumer_group(GroupConsumerCloseTerminal::Succeeded);
                if !resolved {
                    return Err(ConsumerGroupExecutionError::EffectShape);
                }
            }
            return Ok(ConsumerGroupCloseTurn::Progress);
        }
        Ok(if blocked {
            ConsumerGroupCloseTurn::Blocked
        } else {
            ConsumerGroupCloseTurn::Idle
        })
    }

    pub(super) fn recover_consumer_groups_after_driver_shutdown(
        &mut self,
    ) -> Result<(), ConsumerGroupExecutionError> {
        for entry in &mut self.entries {
            if !entry.uses_consumer_group_protocol() {
                continue;
            }
            entry
                .consumer
                .as_mut()
                .ok_or(ConsumerGroupExecutionError::EffectShape)?
                .discard_calls_after_driver_shutdown();
            drop(entry.consumer_reconciliation.take());
            if entry.consumer_revocation.is_some() {
                continue;
            }
            if entry.consumer.as_ref().is_some_and(|execution| {
                execution.machine().phase() != ConsumerGroupHeartbeatPhase::Closed
            }) {
                close_consumer_group_locally(entry, GroupConsumerCloseTerminal::Succeeded)?;
            }
        }
        Ok(())
    }

    pub(super) fn close_one_pending_consumer_reconciliation_after_driver_shutdown(
        &mut self,
    ) -> Result<bool, ConsumerGroupExecutionError> {
        let Some(index) = self.entries.iter().position(|entry| {
            entry.uses_consumer_group_protocol()
                && entry.consumer_revocation.is_none()
                && entry.consumer_reconciliation.is_some()
        }) else {
            return Ok(false);
        };
        drop(self.entries[index].consumer_reconciliation.take());
        close_consumer_group_locally(
            &mut self.entries[index],
            GroupConsumerCloseTerminal::Succeeded,
        )?;
        Ok(true)
    }
}

pub(super) fn complete_consumer_group_leave(
    entry: &mut GroupConsumerEntry,
) -> Result<(), ConsumerGroupExecutionError> {
    let revoked = entry
        .consumer
        .as_mut()
        .ok_or(ConsumerGroupExecutionError::EffectShape)?
        .apply_leave_success()?;
    drop(entry.consumer_reconciliation.take());
    stage_consumer_group_revocation(entry, revoked)?;
    if !entry
        .leave
        .resolve_consumer_group(GroupConsumerCloseTerminal::Succeeded)
    {
        return Err(ConsumerGroupExecutionError::EffectShape);
    }
    Ok(())
}

pub(super) fn fail_consumer_group_leave(
    entry: &mut GroupConsumerEntry,
    failure: ConsumerGroupHeartbeatFailure,
    terminal: GroupConsumerCloseTerminal,
) -> Result<(), ConsumerGroupExecutionError> {
    let revoked = entry
        .consumer
        .as_mut()
        .ok_or(ConsumerGroupExecutionError::EffectShape)?
        .apply_current_failure(failure)?;
    finish_consumer_group_leave_failure(entry, revoked, terminal)
}

pub(super) fn finish_consumer_group_leave_failure(
    entry: &mut GroupConsumerEntry,
    revoked: Option<kafka_client_core::LiveGroupAssignment>,
    terminal: GroupConsumerCloseTerminal,
) -> Result<(), ConsumerGroupExecutionError> {
    drop(entry.consumer_reconciliation.take());
    stage_consumer_group_revocation(entry, revoked)?;
    close_consumer_group_locally(entry, terminal)
}

pub(super) const fn deadline_terminal() -> GroupConsumerCloseTerminal {
    GroupConsumerCloseTerminal::Failed(GroupConsumerCloseTerminalFailure {
        kind: GroupConsumerCloseTerminalFailureKind::DeadlineElapsed,
        broker_code: None,
    })
}

fn close_consumer_group_locally(
    entry: &mut GroupConsumerEntry,
    terminal: GroupConsumerCloseTerminal,
) -> Result<(), ConsumerGroupExecutionError> {
    let revoked = entry
        .consumer
        .as_mut()
        .ok_or(ConsumerGroupExecutionError::EffectShape)?
        .close_locally()?;
    drop(entry.consumer_reconciliation.take());
    stage_consumer_group_revocation(entry, revoked)?;
    if entry.leave.pending_deadline().is_some() && !entry.leave.resolve_consumer_group(terminal) {
        return Err(ConsumerGroupExecutionError::EffectShape);
    }
    Ok(())
}

fn close_terminal(entry: &GroupConsumerEntry, now: Moment) -> GroupConsumerCloseTerminal {
    if entry
        .leave
        .pending_deadline()
        .is_some_and(|deadline| deadline.core().is_elapsed_at(now))
    {
        deadline_terminal()
    } else {
        GroupConsumerCloseTerminal::Succeeded
    }
}
