//! Successful KIP-848 heartbeat validation and atomic assignment-catalog installation.

mod reconciliation;

use kafka_client_core::{
    ConsumerGroupHeartbeatEffect, ConsumerGroupHeartbeatFailure, ConsumerGroupHeartbeatInput,
    ConsumerGroupMemberEpoch, Moment,
};

use super::{
    consumer_group_assignment_install::{
        PreparedConsumerGroupAssignmentInstall, install_consumer_group_assignment,
        install_reconciled_consumer_group_assignment,
    },
    consumer_group_execution::ConsumerGroupExecutionError,
    consumer_group_execution_terminal::fail_consumer_group_entry,
    consumer_group_heartbeat_settlement::{
        ConsumerGroupHeartbeatSettlementTurn, fail_invalid_heartbeat,
    },
    registry_entry::GroupConsumerEntry,
    registry_graceful_revocation::consumer_group::{
        prepare_reconciliation_revocation_deadline, stage_consumer_group_reconciliation,
    },
};

use self::reconciliation::reconciliation_core_matches;

const TICKS_PER_MILLISECOND: u64 = 1_000_000;

#[expect(clippy::too_many_lines, reason = "atomic heartbeat settlement")]
pub(super) fn settle_success(
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
        }) if installed_epoch == member_epoch
            && schedule.assignment_generation()
                == previous
                    .as_ref()
                    .map_or(assignment.assignment_generation(), |previous| {
                        previous.assignment_generation()
                    }) =>
        {
            if !reconciliation_core_matches(entry, previous.as_ref(), &assignment) {
                return Err(ConsumerGroupExecutionError::EffectShape);
            }
            let cycle = install_cycle.ok_or(ConsumerGroupExecutionError::EffectShape)?;
            match previous {
                None if !replaces_live_assignment => {
                    let install = PreparedConsumerGroupAssignmentInstall::new(
                        candidate,
                        cycle,
                        member_epoch,
                        assignment,
                        prepared.deadline(),
                        now,
                    );
                    install_consumer_group_assignment(entry, install)?;
                }
                Some(previous)
                    if replaces_live_assignment
                        && entry.catalog.live_assignment() == Some(&previous)
                        && entry.consumer_reconciliation.is_none() =>
                {
                    let install = PreparedConsumerGroupAssignmentInstall::new(
                        candidate,
                        cycle,
                        member_epoch,
                        assignment,
                        prepared.deadline(),
                        now,
                    );
                    stage_consumer_group_reconciliation(
                        entry,
                        previous,
                        install,
                        revocation_deadline,
                        now,
                    )?;
                }
                _ => return Err(ConsumerGroupExecutionError::EffectShape),
            }
        }
        Some(ConsumerGroupHeartbeatEffect::ArmHeartbeat { schedule })
            if entry.catalog.current_member_id() == Some(candidate.member_id())
                && entry.consumer.as_ref().is_some_and(|execution| {
                    execution.machine().member_epoch() == Some(member_epoch)
                })
                && entry.consumer.as_ref().is_some_and(|execution| {
                    let reportable = execution.machine().live_assignment();
                    entry.catalog.live_assignment() == reportable
                        && reportable.is_some_and(|assignment| {
                            assignment.assignment_generation() == schedule.assignment_generation()
                        })
                }) =>
        {
            if entry.catalog.consumer_group_member_epoch() != Some(member_epoch) {
                entry
                    .catalog
                    .commit_consumer_group_reconciliation_epoch(&candidate, member_epoch);
            }
        }
        Some(ConsumerGroupHeartbeatEffect::InstallReconciled {
            member_id,
            member_epoch: installed_epoch,
            assignment_generation,
            schedule,
        }) if member_id == candidate.member_id()
            && installed_epoch == member_epoch
            && schedule.assignment_generation() == assignment_generation
            && entry.catalog.current_member_id() == Some(member_id)
            && entry.catalog.consumer_group_member_epoch() == Some(member_epoch)
            && entry.catalog.live_assignment().is_none()
            && entry.consumer_revocation.is_none()
            && entry.consumer.as_ref().is_some_and(|execution| {
                execution.machine().pending_assignment().is_none()
                    && execution
                        .machine()
                        .live_assignment()
                        .is_some_and(|assignment| {
                            assignment.member_id() == member_id
                                && assignment.assignment_generation() == assignment_generation
                        })
            }) =>
        {
            let install = entry
                .consumer_reconciliation
                .take()
                .ok_or(ConsumerGroupExecutionError::EffectShape)?;
            if install.member_id() != member_id
                || install.member_epoch() != member_epoch
                || install.assignment().assignment_generation() != assignment_generation
            {
                entry.consumer_reconciliation = Some(install);
                return Err(ConsumerGroupExecutionError::EffectShape);
            }
            let install = install.refresh_resolution_boundary(prepared.deadline(), now);
            install_reconciled_consumer_group_assignment(entry, install)?;
        }
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
