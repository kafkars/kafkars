//! Atomic assignment revocation and recovery installation after Heartbeat rejection.

use kafka_client_core::{
    ClassicCoordinatorRecovery, ClassicGeneration, ClassicGroupEffect, ClassicGroupFatal,
    ClassicGroupPhase, ClassicRejoinSchedule, LiveGroupAssignment,
};

use super::{
    classic_group_assignment::{
        ClassicGroupRevocationFailureKind, retire_and_revoke_classic_group_assignment,
    },
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
    if let Err(failure) = retire_and_revoke_classic_group_assignment(
        &entry.classic,
        &mut entry.catalog,
        &mut entry.processing_lease,
        &mut entry.fetch,
        assignment,
        generation,
    ) {
        drop(prepared_rejoin);
        let kind = rejection_revocation_kind(failure.kind);
        return Err(post_rejoin(
            failure.assignment,
            failure.classic_generation,
            schedule,
            kind,
        ));
    }
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
    if let Err(failure) = retire_and_revoke_classic_group_assignment(
        &entry.classic,
        &mut entry.catalog,
        &mut entry.processing_lease,
        &mut entry.fetch,
        assignment,
        generation,
    ) {
        drop(prepared_rejoin);
        drop(prepared_rediscovery);
        let kind = rejection_revocation_kind(failure.kind);
        return Err(post_rediscovery(
            failure.assignment,
            failure.classic_generation,
            schedule,
            kind,
        ));
    }
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
    match retire_and_revoke_classic_group_assignment(
        &entry.classic,
        &mut entry.catalog,
        &mut entry.processing_lease,
        &mut entry.fetch,
        assignment,
        generation,
    ) {
        Ok(_retirement) => Ok(()),
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

const fn rejection_revocation_kind(
    kind: ClassicGroupRevocationFailureKind,
) -> ClassicRejectionInstallFailure {
    match kind {
        ClassicGroupRevocationFailureKind::Catalog(kind) => Assignment(kind),
        ClassicGroupRevocationFailureKind::ProcessingLeaseCycleUnavailable => {
            ProcessingLeaseCycleUnavailable
        }
        ClassicGroupRevocationFailureKind::ProcessingLease(error) => ProcessingLease(error),
        ClassicGroupRevocationFailureKind::Fetch(error) => FetchRetirement(error),
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

use ClassicRejectionInstallFailure::{
    Assignment, FetchRetirement, MachineState, ProcessingLease, ProcessingLeaseCycleUnavailable,
    RediscoveryState, RejoinState,
};
