//! Initial API 68 terminal normalization, core transition, and catalog install.

use kafka_client_core::{
    ConsumerGroupHeartbeatEffect, ConsumerGroupHeartbeatFailure, ConsumerGroupHeartbeatInput,
    ConsumerGroupHeartbeatRequestKind, ConsumerGroupMemberEpoch, Moment,
};

use crate::driver::ConsumerGroupHeartbeatResolution;

use super::{
    consumer_group_execution::ConsumerGroupExecutionError,
    consumer_group_execution_terminal::fail_consumer_group_entry,
    consumer_group_heartbeat_failure::{completion_failure, driver_failure},
    consumer_group_heartbeat_leave_settlement::{
        settle_leave_completion_error, settle_leave_resolution,
    },
    registry::GroupConsumerRegistry,
    registry_entry::GroupConsumerEntry,
};

const TICKS_PER_MILLISECOND: u64 = 1_000_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ConsumerGroupHeartbeatSettlementTurn {
    Idle,
    Progress,
    Blocked,
}

impl GroupConsumerRegistry {
    pub(super) fn settle_one_consumer_group_heartbeat(
        &mut self,
        now: Moment,
    ) -> Result<ConsumerGroupHeartbeatSettlementTurn, ConsumerGroupExecutionError> {
        let Some(index) = self.entries.iter().position(|entry| {
            entry
                .consumer
                .as_ref()
                .is_some_and(|execution| execution.heartbeat_call().is_some())
        }) else {
            return Ok(ConsumerGroupHeartbeatSettlementTurn::Idle);
        };
        settle_heartbeat(&mut self.entries[index], now)
    }
}

fn settle_heartbeat(
    entry: &mut GroupConsumerEntry,
    now: Moment,
) -> Result<ConsumerGroupHeartbeatSettlementTurn, ConsumerGroupExecutionError> {
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
    route.accept();
    if kind == ConsumerGroupHeartbeatRequestKind::Leave {
        return settle_leave_resolution(entry, resolution);
    }
    match resolution {
        ConsumerGroupHeartbeatResolution::Succeeded(success) => settle_success(entry, now, success),
        ConsumerGroupHeartbeatResolution::BrokerRejected { error_code, .. } => {
            fail_consumer_group_entry(entry, ConsumerGroupHeartbeatFailure::Broker(error_code))?;
            Ok(ConsumerGroupHeartbeatSettlementTurn::Progress)
        }
        ConsumerGroupHeartbeatResolution::Failed(failure) => {
            fail_consumer_group_entry(entry, driver_failure(failure))?;
            Ok(ConsumerGroupHeartbeatSettlementTurn::Progress)
        }
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "one successful heartbeat atomically validates broker identity, core state, and catalog effects"
)]
fn settle_success(
    entry: &mut GroupConsumerEntry,
    now: Moment,
    success: crate::protocol::consumer::ConsumerGroupHeartbeatSuccess,
) -> Result<ConsumerGroupHeartbeatSettlementTurn, ConsumerGroupExecutionError> {
    let (throttle_time_ms, member, member_epoch, heartbeat_interval_ms, assignment) =
        success.into_parts();
    let prepared = entry
        .consumer
        .as_ref()
        .and_then(|execution| execution.prepared())
        .ok_or(ConsumerGroupExecutionError::MissingPrepared)?;
    let candidate = match member {
        Some(member) => match entry.catalog.prepare_consumer_group_member(member) {
            Ok(candidate) => candidate,
            Err(_error) => return fail_invalid_heartbeat(entry),
        },
        None if prepared.kind() == kafka_client_core::ConsumerGroupHeartbeatRequestKind::Steady => {
            match entry.catalog.current_consumer_group_member_candidate() {
                Some(candidate) => candidate,
                None => return fail_invalid_heartbeat(entry),
            }
        }
        None => return fail_invalid_heartbeat(entry),
    };
    let Some(member_epoch) = ConsumerGroupMemberEpoch::try_from_raw(member_epoch) else {
        return fail_invalid_heartbeat(entry);
    };
    let heartbeat_interval_ticks = u64::from(heartbeat_interval_ms)
        .checked_mul(TICKS_PER_MILLISECOND)
        .ok_or(ConsumerGroupExecutionError::EffectShape)?;
    let throttle_ticks = u64::from(throttle_time_ms)
        .checked_mul(TICKS_PER_MILLISECOND)
        .ok_or(ConsumerGroupExecutionError::EffectShape)?;
    if assignment.is_some() && entry.catalog.live_assignment().is_some() {
        return fail_invalid_heartbeat(entry);
    }
    let assignment = match assignment {
        Some(assignment) => match entry
            .consumer
            .as_ref()
            .ok_or(ConsumerGroupExecutionError::MissingPrepared)?
            .topic_identities()
            .translate_assignment(&assignment)
        {
            Ok(assignment) => Some(assignment),
            Err(_error) => return fail_invalid_heartbeat(entry),
        },
        None => None,
    };
    let install_cycle = assignment.as_ref().and_then(|_assignment| {
        entry
            .consumer
            .as_ref()
            .and_then(|execution| execution.next_reconcile_cycle(false))
    });
    let transition = match entry
        .consumer
        .as_mut()
        .ok_or(ConsumerGroupExecutionError::MissingPrepared)?
        .machine_mut()
        .apply(ConsumerGroupHeartbeatInput::HeartbeatSucceeded {
            attempt: prepared.attempt(),
            now,
            member_id: candidate.member_id(),
            member_epoch,
            heartbeat_interval_ticks,
            throttle_ticks,
            assignment,
        }) {
        Ok(transition) => transition,
        Err(error)
            if error.kind()
                == kafka_client_core::ConsumerGroupHeartbeatErrorKind::DeadlineElapsed =>
        {
            fail_consumer_group_entry(entry, ConsumerGroupHeartbeatFailure::DeadlineElapsed)?;
            return Ok(ConsumerGroupHeartbeatSettlementTurn::Progress);
        }
        Err(_error) => return fail_invalid_heartbeat(entry),
    };
    let mut effects = transition.into_effects();
    match effects.next() {
        Some(ConsumerGroupHeartbeatEffect::Reconcile {
            previous,
            assignment,
            member_epoch: installed_epoch,
            schedule,
        }) if previous.is_none()
            && installed_epoch == member_epoch
            && schedule.assignment_generation() == assignment.assignment_generation() =>
        {
            let cycle = install_cycle.ok_or(ConsumerGroupExecutionError::EffectShape)?;
            entry
                .catalog
                .commit_consumer_group_install(candidate, cycle, member_epoch, assignment);
            entry
                .consumer
                .as_mut()
                .ok_or(ConsumerGroupExecutionError::MissingPrepared)?
                .commit_reconcile_cycle(cycle);
        }
        Some(ConsumerGroupHeartbeatEffect::ArmHeartbeat { schedule })
            if entry.catalog.current_member_id() == Some(candidate.member_id())
                && entry.catalog.consumer_group_member_epoch() == Some(member_epoch)
                && entry.catalog.live_assignment().is_some_and(|assignment| {
                    assignment.assignment_generation() == schedule.assignment_generation()
                }) => {}
        _ => return Err(ConsumerGroupExecutionError::EffectShape),
    }
    if effects.next().is_some() {
        return Err(ConsumerGroupExecutionError::EffectShape);
    }
    if entry
        .consumer
        .as_mut()
        .and_then(|execution| execution.take_prepared())
        != Some(prepared)
    {
        return Err(ConsumerGroupExecutionError::EffectShape);
    }
    Ok(ConsumerGroupHeartbeatSettlementTurn::Progress)
}

fn fail_invalid_heartbeat(
    entry: &mut GroupConsumerEntry,
) -> Result<ConsumerGroupHeartbeatSettlementTurn, ConsumerGroupExecutionError> {
    fail_consumer_group_entry(entry, ConsumerGroupHeartbeatFailure::InvalidResponse)?;
    Ok(ConsumerGroupHeartbeatSettlementTurn::Progress)
}
