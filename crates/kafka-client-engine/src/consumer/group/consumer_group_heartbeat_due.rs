//! One broker-paced KIP-848 cadence observation per membership turn.

use kafka_client_core::{ConsumerGroupHeartbeatFailure, ConsumerGroupHeartbeatRequestKind, Moment};

use crate::clock::MonotonicClock;

use super::{
    consumer_group_assignment_retirement::stage_consumer_group_revocation,
    consumer_group_close::{
        fail_consumer_group_leave, position_failure_allows_consumer_group_leave,
    },
    consumer_group_execution::{ConsumerGroupExecutionError, ConsumerGroupRediscoveryState},
    consumer_group_execution_cadence::ConsumerGroupCoordinatorLoadRetryTurn,
    consumer_group_execution_terminal::fail_consumer_group_entry,
    consumer_group_heartbeat_failure::deadline_terminal,
    registry::GroupConsumerRegistry,
    registry_entry::{GroupConsumerEntry, GroupConsumerEntryState},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ConsumerGroupHeartbeatDueTurn {
    Idle,
    Progress,
}

pub(super) fn settle_one_consumer_group_rediscovery_deadline(
    registry: &mut GroupConsumerRegistry,
    now: Moment,
) -> Result<bool, ConsumerGroupExecutionError> {
    let Some(index) = registry.entries.iter().position(|entry| {
        entry.consumer.as_ref().is_some_and(|execution| {
            execution.rediscovery_state()
                == ConsumerGroupRediscoveryState::AwaitingInvalidationAdmission
                && execution
                    .prepared()
                    .is_some_and(|prepared| prepared.deadline().core().is_elapsed_at(now))
        })
    }) else {
        return Ok(false);
    };
    let execution = registry.entries[index]
        .consumer
        .as_ref()
        .ok_or(ConsumerGroupExecutionError::MissingPrepared)?;
    if execution.machine().retry_schedule().is_some() {
        let turn = registry.entries[index]
            .consumer
            .as_mut()
            .ok_or(ConsumerGroupExecutionError::MissingPrepared)?
            .prepare_due_coordinator_load_retry(now)?;
        settle_consumer_group_load_retry_turn(&mut registry.entries[index], turn)?;
    } else if execution
        .prepared()
        .is_some_and(|prepared| prepared.kind() == ConsumerGroupHeartbeatRequestKind::Leave)
    {
        fail_consumer_group_leave(
            &mut registry.entries[index],
            ConsumerGroupHeartbeatFailure::DeadlineElapsed,
            deadline_terminal(),
        )?;
    } else {
        fail_consumer_group_entry(
            &mut registry.entries[index],
            ConsumerGroupHeartbeatFailure::DeadlineElapsed,
        )?;
    }
    Ok(true)
}

impl GroupConsumerRegistry {
    pub(super) fn prepare_one_consumer_group_load_retry(
        &mut self,
        now: Moment,
    ) -> Result<ConsumerGroupHeartbeatDueTurn, ConsumerGroupExecutionError> {
        let Some(index) = self.entries.iter().position(|entry| {
            (entry.is_active() || entry.state == GroupConsumerEntryState::Closing)
                && (entry.fault.is_none() || position_failure_allows_consumer_group_leave(entry))
                && entry.consumer.as_ref().is_some_and(|execution| {
                    execution
                        .machine()
                        .retry_schedule()
                        .is_some_and(|schedule| schedule.not_before().is_elapsed_at(now))
                })
        }) else {
            return Ok(ConsumerGroupHeartbeatDueTurn::Idle);
        };
        let turn = self.entries[index]
            .consumer
            .as_mut()
            .ok_or(ConsumerGroupExecutionError::MissingPrepared)?
            .prepare_due_coordinator_load_retry(now)?;
        settle_consumer_group_load_retry_turn(&mut self.entries[index], turn)?;
        Ok(ConsumerGroupHeartbeatDueTurn::Progress)
    }

    pub(super) fn prepare_one_consumer_group_heartbeat(
        &mut self,
        now: Moment,
        clock: &MonotonicClock,
    ) -> Result<ConsumerGroupHeartbeatDueTurn, ConsumerGroupExecutionError> {
        let Some(index) = self.entries.iter().position(|entry| {
            entry.is_active()
                && entry.fault.is_none()
                && entry.consumer.as_ref().is_some_and(|execution| {
                    execution
                        .machine()
                        .schedule()
                        .is_some_and(|schedule| schedule.deadline().is_elapsed_at(now))
                })
        }) else {
            return Ok(ConsumerGroupHeartbeatDueTurn::Idle);
        };
        let progressed = self.entries[index]
            .consumer
            .as_mut()
            .ok_or(ConsumerGroupExecutionError::MissingPrepared)?
            .prepare_due_heartbeat(now, clock)?;
        Ok(if progressed {
            ConsumerGroupHeartbeatDueTurn::Progress
        } else {
            ConsumerGroupHeartbeatDueTurn::Idle
        })
    }
}

pub(super) fn settle_consumer_group_load_retry_turn(
    entry: &mut GroupConsumerEntry,
    turn: ConsumerGroupCoordinatorLoadRetryTurn,
) -> Result<(), ConsumerGroupExecutionError> {
    match turn {
        ConsumerGroupCoordinatorLoadRetryTurn::Scheduled { .. }
        | ConsumerGroupCoordinatorLoadRetryTurn::SubmissionReady => Ok(()),
        ConsumerGroupCoordinatorLoadRetryTurn::Terminal { kind, revoked } => {
            drop(entry.consumer_reconciliation.take());
            if revoked.is_none()
                && entry.catalog.current_member_id().is_some()
                && entry.catalog.live_assignment().is_none()
            {
                entry
                    .catalog
                    .commit_consumer_group_close_without_assignment();
            }
            stage_consumer_group_revocation(entry, revoked)?;
            if kind == kafka_client_core::ConsumerGroupHeartbeatRequestKind::Leave
                && !entry.leave.resolve_consumer_group(deadline_terminal())
            {
                return Err(ConsumerGroupExecutionError::EffectShape);
            }
            Ok(())
        }
        ConsumerGroupCoordinatorLoadRetryTurn::Idle => {
            Err(ConsumerGroupExecutionError::EffectShape)
        }
    }
}
