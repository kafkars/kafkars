//! Initial API 68 terminal normalization, core transition, and catalog install.
use kafka_client_core::{ConsumerGroupHeartbeatFailure, ConsumerGroupHeartbeatRequestKind, Moment};

use crate::{clock::MonotonicClock, driver::ConsumerGroupHeartbeatResolution};

use super::{
    consumer_group_assignment_retirement::stage_consumer_group_revocation,
    consumer_group_execution::ConsumerGroupExecutionError,
    consumer_group_execution_terminal::fail_consumer_group_entry,
    consumer_group_heartbeat_due::{
        settle_consumer_group_load_retry_turn, settle_one_consumer_group_rediscovery_deadline,
    },
    consumer_group_heartbeat_failure::{completion_failure, driver_failure},
    consumer_group_heartbeat_leave_settlement::{
        settle_leave_completion_error, settle_leave_resolution,
    },
    registry::GroupConsumerRegistry,
    registry_entry::GroupConsumerEntry,
};

pub(super) use super::consumer_group_heartbeat_success_settlement::settle_success;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ConsumerGroupHeartbeatSettlementTurn {
    Idle,
    Progress,
    Blocked,
}

pub(super) fn fail_invalid_heartbeat(
    entry: &mut GroupConsumerEntry,
) -> Result<ConsumerGroupHeartbeatSettlementTurn, ConsumerGroupExecutionError> {
    fail_consumer_group_entry(entry, ConsumerGroupHeartbeatFailure::InvalidResponse)?;
    Ok(ConsumerGroupHeartbeatSettlementTurn::Progress)
}

impl GroupConsumerRegistry {
    pub(super) fn settle_one_consumer_group_heartbeat(
        &mut self,
        now: Moment,
        clock: &MonotonicClock,
    ) -> Result<ConsumerGroupHeartbeatSettlementTurn, ConsumerGroupExecutionError> {
        if settle_one_consumer_group_rediscovery_deadline(self, now)? {
            return Ok(ConsumerGroupHeartbeatSettlementTurn::Progress);
        }
        let Some(index) = self.entries.iter().position(|entry| {
            entry
                .consumer
                .as_ref()
                .is_some_and(|execution| execution.heartbeat_call().is_some())
        }) else {
            return Ok(ConsumerGroupHeartbeatSettlementTurn::Idle);
        };
        settle_heartbeat(self, index, now, clock)
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "one explicit dispatch settles route authority and the exact heartbeat terminal atomically"
)]
fn settle_heartbeat(
    registry: &mut GroupConsumerRegistry,
    index: usize,
    now: Moment,
    clock: &MonotonicClock,
) -> Result<ConsumerGroupHeartbeatSettlementTurn, ConsumerGroupExecutionError> {
    let entry = &mut registry.entries[index];
    let execution = entry
        .consumer
        .as_mut()
        .ok_or(ConsumerGroupExecutionError::MissingPrepared)?;
    let kind = execution
        .prepared()
        .ok_or(ConsumerGroupExecutionError::MissingPrepared)?
        .kind();
    let call = execution.take_heartbeat_call()?;
    let Some(terminal) = call.try_result() else {
        execution.restore_heartbeat_call(call)?;
        return Ok(ConsumerGroupHeartbeatSettlementTurn::Blocked);
    };
    let outcome = match terminal {
        Ok(outcome) => outcome,
        Err(error) => {
            if kind == ConsumerGroupHeartbeatRequestKind::Leave {
                settle_leave_completion_error(entry, error)?;
            } else {
                fail_consumer_group_entry(entry, completion_failure(error))?;
            }
            return Ok(ConsumerGroupHeartbeatSettlementTurn::Progress);
        }
    };
    let (resolution, route) = outcome.into_resolution();
    if matches!(
        &resolution,
        ConsumerGroupHeartbeatResolution::BrokerRejected { error_code: 14, .. }
    ) {
        route.accept();
        let turn = entry
            .consumer
            .as_mut()
            .ok_or(ConsumerGroupExecutionError::MissingPrepared)?
            .schedule_current_coordinator_load_retry(
                now,
                ConsumerGroupHeartbeatFailure::Broker(14),
            )?;
        settle_consumer_group_load_retry_turn(entry, turn)?;
        return Ok(ConsumerGroupHeartbeatSettlementTurn::Progress);
    }
    if kind == ConsumerGroupHeartbeatRequestKind::Leave {
        return match resolution {
            ConsumerGroupHeartbeatResolution::BrokerRejected { error_code, .. }
                if matches!(error_code, 15 | 16) =>
            {
                registry.settle_consumer_group_rediscovery(
                    index,
                    now,
                    ConsumerGroupHeartbeatFailure::Broker(error_code),
                    route,
                )?;
                Ok(ConsumerGroupHeartbeatSettlementTurn::Progress)
            }
            ConsumerGroupHeartbeatResolution::Failed(failure)
                if driver_failure(failure)
                    == ConsumerGroupHeartbeatFailure::CoordinatorUnavailable =>
            {
                registry.settle_consumer_group_rediscovery(
                    index,
                    now,
                    ConsumerGroupHeartbeatFailure::CoordinatorUnavailable,
                    route,
                )?;
                Ok(ConsumerGroupHeartbeatSettlementTurn::Progress)
            }
            resolution => {
                route.accept();
                settle_leave_resolution(entry, resolution)
            }
        };
    }
    match resolution {
        ConsumerGroupHeartbeatResolution::Succeeded(success) => {
            route.accept();
            settle_success(entry, now, success)
        }
        ConsumerGroupHeartbeatResolution::BrokerRejected { error_code, .. } => {
            if matches!(error_code, 15 | 16) {
                registry.settle_consumer_group_rediscovery(
                    index,
                    now,
                    ConsumerGroupHeartbeatFailure::Broker(error_code),
                    route,
                )?;
                return Ok(ConsumerGroupHeartbeatSettlementTurn::Progress);
            }
            if kind == ConsumerGroupHeartbeatRequestKind::Steady && matches!(error_code, 25 | 110) {
                route.accept();
                let revoked = entry
                    .consumer
                    .as_mut()
                    .ok_or(ConsumerGroupExecutionError::MissingPrepared)?
                    .recover_current_fenced_membership(
                        now,
                        clock,
                        ConsumerGroupHeartbeatFailure::Broker(error_code),
                    )?;
                if revoked.is_none() {
                    if entry.consumer.as_ref().is_some_and(|execution| {
                        execution.machine().phase()
                            == kafka_client_core::ConsumerGroupHeartbeatPhase::Joining
                    }) {
                        entry
                            .catalog
                            .commit_consumer_group_fenced_without_assignment();
                    } else {
                        entry
                            .catalog
                            .commit_consumer_group_close_without_assignment();
                    }
                }
                drop(entry.consumer_reconciliation.take());
                stage_consumer_group_revocation(entry, revoked)?;
                return Ok(ConsumerGroupHeartbeatSettlementTurn::Progress);
            }
            route.accept();
            fail_consumer_group_entry(entry, ConsumerGroupHeartbeatFailure::Broker(error_code))?;
            Ok(ConsumerGroupHeartbeatSettlementTurn::Progress)
        }
        ConsumerGroupHeartbeatResolution::Failed(failure) => {
            let failure = driver_failure(failure);
            if failure == ConsumerGroupHeartbeatFailure::CoordinatorUnavailable {
                registry.settle_consumer_group_rediscovery(index, now, failure, route)?;
                return Ok(ConsumerGroupHeartbeatSettlementTurn::Progress);
            }
            route.accept();
            fail_consumer_group_entry(entry, failure)?;
            Ok(ConsumerGroupHeartbeatSettlementTurn::Progress)
        }
    }
}
