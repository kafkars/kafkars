//! One exact due classic rejoin transition and fixed-epoch Join staging per host turn.

use kafka_client_core::{
    ClassicGroupEffect, ClassicGroupFatal, ClassicGroupFatalReason, ClassicGroupInput,
    ClassicGroupPhase, ClassicRejoinSchedule, MembershipCycle, Moment,
};

use crate::clock::MonotonicClock;

use super::{
    classic_group_entry_fault::ClassicGroupEntryFault,
    classic_group_execution::ClassicGroupExecutionError,
    classic_group_join::PreparedClassicGroupJoin,
    classic_group_rejoin_fault::{
        ClassicRejoinPostCore, ClassicRejoinPostCoreFailure, PendingClassicRejoinJoin,
    },
    registry::GroupConsumerRegistry,
    registry_entry::{GroupConsumerEntry, GroupConsumerEntryState},
};

/// Result of inspecting the bounded registry for one due rejoin.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ClassicGroupRejoinDueTurn {
    Idle,
    Progress,
}

impl GroupConsumerRegistry {
    pub(super) fn prepare_one_classic_rejoin(
        &mut self,
        now: Moment,
        clock: &MonotonicClock,
    ) -> Result<ClassicGroupRejoinDueTurn, ClassicGroupExecutionError> {
        let Some(index) = due_rejoin_index(&self.entries, now) else {
            return Ok(ClassicGroupRejoinDueTurn::Idle);
        };
        let entry = &mut self.entries[index];
        let schedule = entry
            .rejoin
            .schedule()
            .ok_or(ClassicGroupExecutionError::RejoinState)?;
        if !is_exact_due_state(entry, schedule) {
            return Err(ClassicGroupExecutionError::RejoinState);
        }
        let prior_cycle = schedule.cycle();
        let Some(pending) = apply_due_transition(entry, schedule, now)? else {
            return Ok(ClassicGroupRejoinDueTurn::Progress);
        };
        stage_due_join(entry, schedule, prior_cycle, pending, clock)?;
        Ok(ClassicGroupRejoinDueTurn::Progress)
    }
}

fn due_rejoin_index(entries: &[GroupConsumerEntry], now: Moment) -> Option<usize> {
    entries.iter().position(|entry| {
        entry.state == GroupConsumerEntryState::Active
            && !entry.rediscovery.blocks_join()
            && entry
                .rejoin
                .schedule()
                .is_some_and(|schedule| schedule.due().is_elapsed_at(now))
    })
}

fn is_exact_due_state(entry: &GroupConsumerEntry, schedule: ClassicRejoinSchedule) -> bool {
    entry.execution.is_idle()
        && entry.heartbeat.is_dormant()
        && entry.position.is_dormant()
        && entry.catalog.live_assignment().is_none()
        && entry.classic.pending().is_none()
        && entry.classic.machine().phase() == ClassicGroupPhase::WaitingToRejoin
        && entry.classic.machine().pending_rejoin() == Some(schedule)
        && !entry.rediscovery.blocks_join()
}

fn apply_due_transition(
    entry: &mut GroupConsumerEntry,
    schedule: ClassicRejoinSchedule,
    now: Moment,
) -> Result<Option<PendingClassicRejoinJoin>, ClassicGroupExecutionError> {
    let transition = entry
        .classic
        .apply(ClassicGroupInput::RejoinDue { schedule, now })
        .map_err(|error| ClassicGroupExecutionError::Core(error.kind()))?;
    let mut effects = transition.into_effects();
    let first = effects.next();
    let second = effects.next();
    let pending = match first {
        Some(ClassicGroupEffect::Join {
            group_id,
            cycle,
            protocol,
            timing,
            deadline,
        }) => PendingClassicRejoinJoin::new(group_id, cycle, protocol, timing, deadline),
        Some(ClassicGroupEffect::Fatal { fatal }) if exact_due_fatal(entry, schedule, fatal) => {
            if second.is_some() {
                return freeze(
                    entry,
                    ClassicRejoinPostCore::new(
                        None,
                        [Some(ClassicGroupEffect::Fatal { fatal }), second],
                        EffectShape,
                    ),
                );
            }
            if entry.rejoin.clear_rejoin_exact(schedule).is_err() {
                return freeze(
                    entry,
                    ClassicRejoinPostCore::new(
                        None,
                        [Some(ClassicGroupEffect::Fatal { fatal }), None],
                        ScheduleState,
                    ),
                );
            }
            return Ok(None);
        }
        other => {
            return freeze(
                entry,
                ClassicRejoinPostCore::new(None, [other, second], EffectShape),
            );
        }
    };
    if second.is_some() {
        return freeze(
            entry,
            ClassicRejoinPostCore::new(Some(pending), [None, second], EffectShape),
        );
    }
    Ok(Some(pending))
}

fn exact_due_fatal(
    entry: &GroupConsumerEntry,
    schedule: ClassicRejoinSchedule,
    fatal: ClassicGroupFatal,
) -> bool {
    let fatal_cycle = fatal.cycle();
    let scheduled_cycle = schedule.cycle();
    matches!(
        fatal.reason(),
        ClassicGroupFatalReason::CycleExhausted | ClassicGroupFatalReason::AttemptDeadlineOverflow
    ) && fatal_cycle == scheduled_cycle
        && fatal.assignment_generation() == schedule.assignment_generation()
        && entry.classic.machine().phase() == ClassicGroupPhase::Fatal
        && entry.classic.machine().fatal() == Some(fatal)
        && entry.classic.machine().pending_rejoin().is_none()
        && entry.classic.machine().active_cycle().is_none()
        && entry.classic.machine().deadline().is_none()
}

fn stage_due_join(
    entry: &mut GroupConsumerEntry,
    schedule: ClassicRejoinSchedule,
    prior_cycle: MembershipCycle,
    pending: PendingClassicRejoinJoin,
    clock: &MonotonicClock,
) -> Result<(), ClassicGroupExecutionError> {
    if pending.group_id() != entry.group_id()
        || pending.timing() != entry.classic.machine().timing()
    {
        return freeze(
            entry,
            ClassicRejoinPostCore::new(Some(pending), [None, None], Identity),
        );
    }
    if prior_cycle.checked_next() != Some(pending.cycle()) {
        return freeze(
            entry,
            ClassicRejoinPostCore::new(Some(pending), [None, None], CycleSequence),
        );
    }
    if entry.classic.machine().phase() != ClassicGroupPhase::Joining
        || entry.classic.machine().active_cycle() != Some(pending.cycle())
        || entry.classic.machine().deadline() != Some(pending.deadline())
    {
        return freeze(
            entry,
            ClassicRejoinPostCore::new(Some(pending), [None, None], MachineState),
        );
    }
    let mapped = match clock.operation_deadline(pending.deadline()) {
        Ok(deadline) => deadline,
        Err(error) => {
            return freeze(
                entry,
                ClassicRejoinPostCore::new(Some(pending), [None, None], Clock(error)),
            );
        }
    };
    if entry.rejoin.clear_rejoin_exact(schedule).is_err() {
        return freeze(
            entry,
            ClassicRejoinPostCore::new(Some(pending), [None, None], ScheduleState),
        );
    }
    let prepared = PreparedClassicGroupJoin::new(
        pending.group_id(),
        pending.cycle(),
        pending.protocol(),
        pending.timing(),
        mapped,
    );
    if entry.execution.stage_rejoin_join(prepared).is_err() {
        return freeze(
            entry,
            ClassicRejoinPostCore::new(Some(pending), [None, None], ExecutionOccupied),
        );
    }
    Ok(())
}

fn freeze<T>(
    entry: &mut GroupConsumerEntry,
    fault: ClassicRejoinPostCore,
) -> Result<T, ClassicGroupExecutionError> {
    entry.fault = Some(ClassicGroupEntryFault::RejoinPostCore(fault));
    Err(ClassicGroupExecutionError::RejoinPostCore)
}

use ClassicRejoinPostCoreFailure::{
    Clock, CycleSequence, EffectShape, ExecutionOccupied, Identity, MachineState, ScheduleState,
};
