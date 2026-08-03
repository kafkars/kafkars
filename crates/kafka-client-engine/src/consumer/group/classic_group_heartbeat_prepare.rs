//! Core-authorized heartbeat scheduling, deadline mapping, and local loss.

use std::sync::Arc;

use kafka_client_core::{
    ClassicGeneration, ClassicGroupEffect, ClassicGroupInput, ClassicGroupTransition,
    ClassicHeartbeatAttempt, Moment,
};

use crate::{
    clock::MonotonicClock, driver::classic_group::ClassicHeartbeatCallKey,
    protocol::consumer::classic_heartbeat_request_with_instance,
};

use super::{
    classic_group_assignment::{
        ClassicGroupRevocationFailure, ClassicGroupRevocationFailureKind,
        retire_and_revoke_classic_group_assignment,
    },
    classic_group_entry_fault::ClassicGroupEntryFault,
    classic_group_execution::ClassicGroupExecutionError,
    classic_group_heartbeat::{
        ClassicHeartbeatExecutionError, ClassicHeartbeatExecutionState, PreparedClassicHeartbeat,
    },
    classic_group_reconciliation_loss::stage_classic_group_reconciliation_loss,
    registry::GroupConsumerRegistry,
    registry_entry::GroupConsumerEntry,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ClassicHeartbeatPreparationTurn {
    Idle,
    Progress,
}

impl GroupConsumerRegistry {
    pub(super) fn prepare_one_classic_heartbeat(
        &mut self,
        now: Moment,
        clock: &MonotonicClock,
    ) -> Result<ClassicHeartbeatPreparationTurn, ClassicGroupExecutionError> {
        let Some(index) = self.entries.iter().position(|entry| {
            entry.is_active()
                && matches!(
                    entry.heartbeat.state(),
                    ClassicHeartbeatExecutionState::Waiting(schedule)
                        if schedule.next_deadline().is_elapsed_at(now)
                )
        }) else {
            return Ok(ClassicHeartbeatPreparationTurn::Idle);
        };
        prepare_due_heartbeat(&mut self.entries[index], now, clock)?;
        Ok(ClassicHeartbeatPreparationTurn::Progress)
    }

    pub(super) fn expire_one_prepared_heartbeat(
        &mut self,
        now: Moment,
    ) -> Result<bool, ClassicGroupExecutionError> {
        let Some(index) = self.entries.iter().position(|entry| {
            entry.is_active()
                && matches!(
                    entry.heartbeat.state(),
                    ClassicHeartbeatExecutionState::Prepared(prepared)
                        if prepared.key().deadline().core().is_elapsed_at(now)
                )
        }) else {
            return Ok(false);
        };
        let entry = &mut self.entries[index];
        let attempt = entry
            .heartbeat
            .prepared()
            .ok_or(ClassicGroupExecutionError::HeartbeatState)?
            .key()
            .attempt();
        let transition = entry
            .classic
            .apply(ClassicGroupInput::HeartbeatDeadlineElapsed { attempt, now })
            .map_err(|error| ClassicGroupExecutionError::Core(error.kind()))?;
        commit_local_loss(entry, transition)?;
        entry.heartbeat.clear_local().map_err(map_heartbeat_state)?;
        Ok(true)
    }
}

fn prepare_due_heartbeat(
    entry: &mut GroupConsumerEntry,
    now: Moment,
    clock: &MonotonicClock,
) -> Result<(), ClassicGroupExecutionError> {
    let ClassicHeartbeatExecutionState::Waiting(schedule) = entry.heartbeat.state() else {
        return Err(ClassicGroupExecutionError::HeartbeatState);
    };
    let schedule = *schedule;
    let transition = entry
        .classic
        .apply(ClassicGroupInput::HeartbeatDue {
            attempt: schedule.attempt(),
            now,
        })
        .map_err(|error| ClassicGroupExecutionError::Core(error.kind()))?;
    let mut effects = transition.into_effects();
    match effects.next() {
        Some(ClassicGroupEffect::SubmitHeartbeat {
            group_id,
            attempt,
            member_id,
            classic_generation,
            deadline,
        }) if effects.next().is_none()
            && group_id == entry.group_id()
            && attempt == schedule.attempt()
            && entry.catalog.current_member_id() == Some(member_id)
            && entry.catalog.classic_generation() == Some(classic_generation.get()) =>
        {
            let mapped = match clock.operation_deadline(deadline) {
                Ok(mapped) => mapped,
                Err(_error) => return fail_prepared_heartbeat(entry, attempt),
            };
            let Some(request) = entry.catalog.current_member().and_then(|member| {
                classic_heartbeat_request_with_instance(
                    entry.catalog.group(),
                    member,
                    entry.catalog.group_instance_id().map(Arc::as_ref),
                    classic_generation,
                )
                .ok()
            }) else {
                return fail_prepared_heartbeat(entry, attempt);
            };
            let key = ClassicHeartbeatCallKey::new(group_id, attempt, mapped);
            entry
                .heartbeat
                .set(ClassicHeartbeatExecutionState::Prepared(
                    PreparedClassicHeartbeat::new(key, request),
                ));
            Ok(())
        }
        Some(ClassicGroupEffect::Revoke {
            assignment,
            classic_generation,
        }) if effects.next().is_none() => {
            commit_revoke(entry, assignment, classic_generation).map_err(|failure| {
                let kind = failure.kind;
                entry.fault = Some(ClassicGroupEntryFault::HeartbeatLocalRevoke { failure });
                map_revocation_kind(kind)
            })?;
            entry.heartbeat.clear_local().map_err(map_heartbeat_state)
        }
        _ => Err(ClassicGroupExecutionError::HeartbeatTerminal),
    }
}

fn fail_prepared_heartbeat(
    entry: &mut GroupConsumerEntry,
    attempt: ClassicHeartbeatAttempt,
) -> Result<(), ClassicGroupExecutionError> {
    let transition = entry
        .classic
        .apply(ClassicGroupInput::HeartbeatFailed { attempt })
        .map_err(|error| ClassicGroupExecutionError::Core(error.kind()))?;
    commit_local_loss(entry, transition)?;
    entry.heartbeat.clear_local().map_err(map_heartbeat_state)
}

pub(super) fn commit_local_loss(
    entry: &mut GroupConsumerEntry,
    transition: ClassicGroupTransition,
) -> Result<(), ClassicGroupExecutionError> {
    let mut effects = transition.into_effects();
    let Some(ClassicGroupEffect::Revoke {
        assignment,
        classic_generation,
    }) = effects.next()
    else {
        return Err(ClassicGroupExecutionError::HeartbeatTerminal);
    };
    if effects.next().is_some() {
        return Err(ClassicGroupExecutionError::HeartbeatTerminal);
    }
    commit_revoke(entry, assignment, classic_generation).map_err(|failure| {
        let kind = failure.kind;
        entry.fault = Some(ClassicGroupEntryFault::HeartbeatLocalRevoke { failure });
        map_revocation_kind(kind)
    })
}

#[expect(
    clippy::result_large_err,
    reason = "the failure retains the exact assignment and generation for lossless recovery"
)]
pub(super) fn commit_revoke(
    entry: &mut GroupConsumerEntry,
    assignment: kafka_client_core::LiveGroupAssignment,
    generation: ClassicGeneration,
) -> Result<(), ClassicGroupRevocationFailure> {
    if entry.classic_reconciliation.is_some() {
        return stage_classic_group_reconciliation_loss(entry, assignment, generation);
    }
    retire_and_revoke_classic_group_assignment(
        &entry.classic,
        &mut entry.catalog,
        &mut entry.processing_lease,
        &mut entry.fetch,
        assignment,
        generation,
    )
    .map(|_retirement| ())
}

fn map_heartbeat_state(_error: ClassicHeartbeatExecutionError) -> ClassicGroupExecutionError {
    ClassicGroupExecutionError::HeartbeatState
}

pub(super) const fn map_revocation_kind(
    kind: ClassicGroupRevocationFailureKind,
) -> ClassicGroupExecutionError {
    match kind {
        ClassicGroupRevocationFailureKind::Catalog(kind) => {
            ClassicGroupExecutionError::Assignment(kind)
        }
        ClassicGroupRevocationFailureKind::ProcessingLeaseCycleUnavailable => {
            ClassicGroupExecutionError::MissingCycle
        }
        ClassicGroupRevocationFailureKind::ProcessingLease(error) => {
            ClassicGroupExecutionError::ProcessingLease(error)
        }
        ClassicGroupRevocationFailureKind::Fetch(error) => {
            ClassicGroupExecutionError::FetchRetirement(error)
        }
    }
}
