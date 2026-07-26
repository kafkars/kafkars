//! Atomic assignment revocation and recovery installation after Heartbeat rejection.

use kafka_client_core::{
    ClassicCoordinatorRecovery, ClassicGeneration, ClassicGroupEffect, ClassicGroupFatal,
    ClassicGroupPhase, ClassicGroupTransition, ClassicRejoinSchedule, LiveGroupAssignment,
};

use super::{
    classic_group_rejection_fault::{ClassicRejectionInstallFailure, ClassicRejectionPostCore},
    registry_entry::GroupConsumerEntry,
};

#[expect(
    clippy::result_large_err,
    reason = "the error retains exact post-core effects without allocating or erasing recovery state"
)]
pub(super) fn install_heartbeat_rejection(
    entry: &mut GroupConsumerEntry,
    transition: ClassicGroupTransition,
) -> Result<(), ClassicRejectionPostCore> {
    let effects = into_effects(transition);
    match effects {
        [
            Some(ClassicGroupEffect::Revoke {
                assignment,
                classic_generation,
            }),
            Some(ClassicGroupEffect::ArmRejoin {
                schedule,
                coordinator: ClassicCoordinatorRecovery::Retain,
            }),
        ] => install_rejoin(entry, assignment, classic_generation, schedule),
        [
            Some(ClassicGroupEffect::Revoke {
                assignment,
                classic_generation,
            }),
            Some(ClassicGroupEffect::ArmRejoin {
                schedule,
                coordinator: ClassicCoordinatorRecovery::Rediscover,
            }),
        ] => Err(ClassicRejectionPostCore::heartbeat(
            assignment,
            classic_generation,
            ClassicGroupEffect::ArmRejoin {
                schedule,
                coordinator: ClassicCoordinatorRecovery::Rediscover,
            },
            CoordinatorRediscovery,
        )),
        [
            Some(ClassicGroupEffect::Revoke {
                assignment,
                classic_generation,
            }),
            Some(ClassicGroupEffect::Fatal { fatal }),
        ] => install_fatal(entry, assignment, classic_generation, fatal),
        effects => Err(ClassicRejectionPostCore::new(effects, EffectShape)),
    }
}

#[expect(
    clippy::result_large_err,
    reason = "the error retains exact post-core effects without allocating or erasing recovery state"
)]
fn install_rejoin(
    entry: &mut GroupConsumerEntry,
    assignment: LiveGroupAssignment,
    generation: ClassicGeneration,
    schedule: ClassicRejoinSchedule,
) -> Result<(), ClassicRejectionPostCore> {
    if !waiting_state_matches(entry, schedule) {
        return Err(post_rejoin(assignment, generation, schedule, MachineState));
    }
    let prepared_rejoin = match entry.rejoin.prepare_rejoin_install(schedule) {
        Ok(prepared) => prepared,
        Err(_error) => {
            return Err(post_rejoin(assignment, generation, schedule, RejoinState));
        }
    };
    let prepared_revoke =
        match entry
            .classic
            .prepare_revoke(&mut entry.catalog, assignment, generation)
        {
            Ok(prepared) => prepared,
            Err(failure) => {
                drop(prepared_rejoin);
                let kind = failure.kind;
                return Err(post_rejoin(
                    failure.assignment,
                    generation,
                    schedule,
                    Assignment(kind),
                ));
            }
        };
    prepared_revoke.commit();
    prepared_rejoin.commit();
    Ok(())
}

#[expect(
    clippy::result_large_err,
    reason = "the error retains exact post-core effects without allocating or erasing recovery state"
)]
fn install_fatal(
    entry: &mut GroupConsumerEntry,
    assignment: LiveGroupAssignment,
    generation: ClassicGeneration,
    fatal: ClassicGroupFatal,
) -> Result<(), ClassicRejectionPostCore> {
    if !fatal_state_matches(entry, fatal) {
        return Err(post_fatal(assignment, generation, fatal, MachineState));
    }
    match entry
        .classic
        .prepare_revoke(&mut entry.catalog, assignment, generation)
    {
        Ok(prepared) => {
            prepared.commit();
            Ok(())
        }
        Err(failure) => {
            let kind = failure.kind;
            Err(post_fatal(
                failure.assignment,
                generation,
                fatal,
                Assignment(kind),
            ))
        }
    }
}

fn waiting_state_matches(entry: &GroupConsumerEntry, schedule: ClassicRejoinSchedule) -> bool {
    entry.classic.machine().phase() == ClassicGroupPhase::WaitingToRejoin
        && entry.classic.machine().pending_rejoin() == Some(schedule)
        && entry.classic.machine().fatal().is_none()
        && entry.rejoin.is_dormant()
}

fn fatal_state_matches(entry: &GroupConsumerEntry, fatal: ClassicGroupFatal) -> bool {
    entry.classic.machine().phase() == ClassicGroupPhase::Fatal
        && entry.classic.machine().fatal() == Some(fatal)
        && entry.classic.machine().pending_rejoin().is_none()
        && entry.rejoin.is_dormant()
}

fn into_effects(transition: ClassicGroupTransition) -> [Option<ClassicGroupEffect>; 2] {
    let mut effects = transition.into_effects();
    [effects.next(), effects.next()]
}

fn post_rejoin(
    assignment: LiveGroupAssignment,
    generation: ClassicGeneration,
    schedule: ClassicRejoinSchedule,
    failure: ClassicRejectionInstallFailure,
) -> ClassicRejectionPostCore {
    ClassicRejectionPostCore::heartbeat(
        assignment,
        generation,
        ClassicGroupEffect::ArmRejoin {
            schedule,
            coordinator: ClassicCoordinatorRecovery::Retain,
        },
        failure,
    )
}

fn post_fatal(
    assignment: LiveGroupAssignment,
    generation: ClassicGeneration,
    fatal: ClassicGroupFatal,
    failure: ClassicRejectionInstallFailure,
) -> ClassicRejectionPostCore {
    ClassicRejectionPostCore::heartbeat(
        assignment,
        generation,
        ClassicGroupEffect::Fatal { fatal },
        failure,
    )
}

use ClassicRejectionInstallFailure::{
    Assignment, CoordinatorRediscovery, EffectShape, MachineState, RejoinState,
};
