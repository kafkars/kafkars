//! Atomic assignment revocation and recovery installation after Heartbeat rejection.

use kafka_client_core::{
    ClassicCoordinatorRecovery, ClassicGeneration, ClassicGroupEffect, ClassicGroupFatal,
    ClassicGroupPhase, ClassicRejoinSchedule, LiveGroupAssignment,
};

use super::{
    classic_group_rejection_fault::{ClassicRejectionInstallFailure, ClassicRejectionPostCore},
    registry_entry::GroupConsumerEntry,
};

#[expect(
    clippy::result_large_err,
    reason = "the error retains exact post-core effects without allocating or erasing recovery state"
)]
pub(super) fn install_rejoin(
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
pub(super) fn install_rediscovery(
    entry: &mut GroupConsumerEntry,
    assignment: LiveGroupAssignment,
    generation: ClassicGeneration,
    schedule: ClassicRejoinSchedule,
) -> Result<(), ClassicRejectionPostCore> {
    if !waiting_state_matches(entry, schedule) {
        return Err(post_rediscovery(
            assignment,
            generation,
            schedule,
            MachineState,
        ));
    }
    let prepared_rediscovery = match entry.rediscovery.prepare_rediscovery_install() {
        Ok(prepared) => prepared,
        Err(_error) => {
            return Err(post_rediscovery(
                assignment,
                generation,
                schedule,
                RediscoveryState,
            ));
        }
    };
    let prepared_rejoin = match entry.rejoin.prepare_rejoin_install(schedule) {
        Ok(prepared) => prepared,
        Err(_error) => {
            return Err(post_rediscovery(
                assignment,
                generation,
                schedule,
                RejoinState,
            ));
        }
    };
    let prepared_revoke =
        match entry
            .classic
            .prepare_revoke(&mut entry.catalog, assignment, generation)
        {
            Ok(prepared) => prepared,
            Err(failure) => {
                let kind = failure.kind;
                return Err(post_rediscovery(
                    failure.assignment,
                    generation,
                    schedule,
                    Assignment(kind),
                ));
            }
        };
    prepared_revoke.commit();
    prepared_rejoin.commit();
    prepared_rediscovery.commit();
    Ok(())
}

#[expect(
    clippy::result_large_err,
    reason = "the error retains exact post-core effects without allocating or erasing recovery state"
)]
pub(super) fn install_fatal(
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
        && !entry.rediscovery.blocks_join()
}

fn fatal_state_matches(entry: &GroupConsumerEntry, fatal: ClassicGroupFatal) -> bool {
    entry.classic.machine().phase() == ClassicGroupPhase::Fatal
        && entry.classic.machine().fatal() == Some(fatal)
        && entry.classic.machine().pending_rejoin().is_none()
        && entry.rejoin.is_dormant()
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

fn post_rediscovery(
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
            coordinator: ClassicCoordinatorRecovery::Rediscover,
        },
        failure,
    )
}

use ClassicRejectionInstallFailure::{Assignment, MachineState, RediscoveryState, RejoinState};
