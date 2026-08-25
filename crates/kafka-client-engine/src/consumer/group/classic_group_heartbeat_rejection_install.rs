//! Heartbeat-loss recovery entry points and exact fatal installation.

mod recovery;
#[cfg(test)]
mod recovery_test;

use kafka_client_core::{
    ClassicCoordinatorRecovery, ClassicGeneration, ClassicGroupEffect, ClassicGroupFatal,
    ClassicGroupPhase, ClassicRejoinSchedule, LiveGroupAssignment, Moment,
};

use super::{
    classic_group_heartbeat_prepare::commit_revoke,
    classic_group_rejection_fault::{ClassicRejectionInstallFailure, ClassicRejectionPostCore},
    registry_entry::GroupConsumerEntry,
};

use self::recovery::{install_recovery, rejection_revocation_kind};

#[expect(
    clippy::result_large_err,
    reason = "the error retains exact post-core effects without allocating or erasing recovery state"
)]
pub(super) fn install_rejoin(
    entry: &mut GroupConsumerEntry,
    assignment: LiveGroupAssignment,
    generation: ClassicGeneration,
    schedule: ClassicRejoinSchedule,
    now: Moment,
) -> Result<(), ClassicRejectionPostCore> {
    install_recovery(
        entry,
        assignment,
        generation,
        schedule,
        now,
        ClassicCoordinatorRecovery::Retain,
    )
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
    now: Moment,
) -> Result<(), ClassicRejectionPostCore> {
    install_recovery(
        entry,
        assignment,
        generation,
        schedule,
        now,
        ClassicCoordinatorRecovery::Rediscover,
    )
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
    match commit_revoke(entry, assignment, generation) {
        Ok(()) => Ok(()),
        Err(failure) => {
            let kind = rejection_revocation_kind(failure.kind);
            Err(post_fatal(
                failure.assignment,
                failure.classic_generation,
                fatal,
                kind,
            ))
        }
    }
}

fn fatal_state_matches(entry: &GroupConsumerEntry, fatal: ClassicGroupFatal) -> bool {
    entry.classic.machine().phase() == ClassicGroupPhase::Fatal
        && entry.classic.machine().fatal() == Some(fatal)
        && entry.classic.machine().pending_rejoin().is_none()
        && entry.rejoin.is_dormant()
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

use ClassicRejectionInstallFailure::MachineState;
