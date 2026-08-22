//! KIP-848 heartbeat success validation before atomic core-effect installation.

use kafka_client_core::{
    ConsumerGroupHeartbeatFailure, ConsumerGroupHeartbeatInput, ConsumerGroupMemberEpoch, Moment,
};

use super::{
    super::{
        consumer_group_execution::{ConsumerGroupExecution, ConsumerGroupExecutionError},
        consumer_group_execution_terminal::fail_consumer_group_entry,
        consumer_group_heartbeat_settlement::{
            ConsumerGroupHeartbeatSettlementTurn, fail_invalid_heartbeat,
        },
        registry_entry::GroupConsumerEntry,
        registry_graceful_revocation::consumer_group::prepare_reconciliation_revocation_deadline,
    },
    effects::{ConsumerGroupSuccessEffectContext, settle_success_effect},
};

const TICKS_PER_MILLISECOND: u64 = 1_000_000;

#[expect(clippy::too_many_lines, reason = "atomic heartbeat settlement")]
pub(in crate::consumer::group) fn settle_success(
    entry: &mut GroupConsumerEntry,
    now: Moment,
    success: crate::protocol::consumer::ConsumerGroupHeartbeatSuccess,
) -> Result<ConsumerGroupHeartbeatSettlementTurn, ConsumerGroupExecutionError> {
    let (throttle_time_ms, member, member_epoch, heartbeat_interval_ms, assignment) =
        success.into_parts();
    let prepared = entry
        .consumer
        .as_ref()
        .and_then(ConsumerGroupExecution::prepared)
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
    let replaces_live_assignment =
        assignment.is_some() && entry.catalog.live_assignment().is_some();
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
    let revocation_deadline =
        prepare_reconciliation_revocation_deadline(entry, now, replaces_live_assignment)?;
    let install_cycle = assignment.as_ref().and_then(|_assignment| {
        entry
            .consumer
            .as_ref()
            .and_then(|execution| execution.next_reconcile_cycle(replaces_live_assignment))
    });
    let current_cycle = entry
        .consumer
        .as_ref()
        .and_then(ConsumerGroupExecution::cycle);
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
    let effect = effects
        .next()
        .ok_or(ConsumerGroupExecutionError::EffectShape)?;
    settle_success_effect(
        entry,
        effect,
        ConsumerGroupSuccessEffectContext {
            candidate,
            member_epoch,
            deadline: prepared.deadline(),
            now,
            replaces_live_assignment,
            install_cycle,
            current_cycle,
            revocation_deadline,
        },
    )?;
    if effects.next().is_some() {
        return Err(ConsumerGroupExecutionError::EffectShape);
    }
    if entry
        .consumer
        .as_mut()
        .and_then(ConsumerGroupExecution::take_prepared)
        != Some(prepared)
    {
        return Err(ConsumerGroupExecutionError::EffectShape);
    }
    Ok(ConsumerGroupHeartbeatSettlementTurn::Progress)
}
