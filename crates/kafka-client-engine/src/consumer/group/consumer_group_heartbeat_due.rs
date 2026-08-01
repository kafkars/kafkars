//! One broker-paced KIP-848 cadence observation per membership turn.

use kafka_client_core::Moment;

use crate::clock::MonotonicClock;

use super::{
    consumer_group_assignment_retirement::stage_consumer_group_revocation,
    consumer_group_close::deadline_terminal,
    consumer_group_execution::ConsumerGroupExecutionError,
    consumer_group_execution_cadence::ConsumerGroupCoordinatorLoadRetryTurn,
    registry::GroupConsumerRegistry,
    registry_entry::{GroupConsumerEntry, GroupConsumerEntryState},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ConsumerGroupHeartbeatDueTurn {
    Idle,
    Progress,
}

impl GroupConsumerRegistry {
    pub(super) fn prepare_one_consumer_group_load_retry(
        &mut self,
        now: Moment,
    ) -> Result<ConsumerGroupHeartbeatDueTurn, ConsumerGroupExecutionError> {
        let Some(index) = self.entries.iter().position(|entry| {
            (entry.is_active() || entry.state == GroupConsumerEntryState::Closing)
                && entry.fault.is_none()
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
