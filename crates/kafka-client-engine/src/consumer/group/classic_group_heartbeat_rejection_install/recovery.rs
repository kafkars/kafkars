//! Atomic revocation and bounded recovery installation after Heartbeat loss.

use kafka_client_core::{
    ClassicCoordinatorRecovery, ClassicGeneration, ClassicGroupEffect, ClassicGroupPhase,
    ClassicRejoinSchedule, LiveGroupAssignment, Moment,
};

use super::super::{
    classic_group_assignment::{
        ClassicGroupRevocationFailureKind, retire_and_revoke_classic_group_assignment,
    },
    classic_group_reconciliation_loss::stage_classic_group_reconciliation_loss,
    classic_group_rejection_fault::{ClassicRejectionInstallFailure, ClassicRejectionPostCore},
    registry_entry::GroupConsumerEntry,
    registry_graceful_revocation::stage_classic_group_revocation,
};

#[expect(
    clippy::result_large_err,
    reason = "the error retains exact post-core effects without allocating or erasing recovery state"
)]
pub(super) fn install_recovery(
    entry: &mut GroupConsumerEntry,
    assignment: LiveGroupAssignment,
    generation: ClassicGeneration,
    schedule: ClassicRejoinSchedule,
    now: Moment,
    coordinator: ClassicCoordinatorRecovery,
) -> Result<(), ClassicRejectionPostCore> {
    if !waiting_state_matches(entry, schedule) {
        return Err(post_recovery(
            assignment,
            generation,
            schedule,
            coordinator,
            MachineState,
        ));
    }
    let prepared_rediscovery = match coordinator {
        ClassicCoordinatorRecovery::Retain => None,
        ClassicCoordinatorRecovery::Rediscover => {
            match entry.rediscovery.prepare_rediscovery_install() {
                Ok(prepared) => Some(prepared),
                Err(_error) => {
                    return Err(post_recovery(
                        assignment,
                        generation,
                        schedule,
                        coordinator,
                        RediscoveryState,
                    ));
                }
            }
        }
    };
    let prepared_rejoin = match entry.rejoin.prepare_rejoin_install(schedule) {
        Ok(prepared) => prepared,
        Err(_error) => {
            return Err(post_recovery(
                assignment,
                generation,
                schedule,
                coordinator,
                RejoinState,
            ));
        }
    };
    if entry.classic_reconciliation.is_some() {
        if let Err(failure) = stage_classic_group_reconciliation_loss(
            &entry.classic,
            &mut entry.catalog,
            &mut entry.classic_reconciliation,
            assignment,
            generation,
        ) {
            drop(prepared_rejoin);
            drop(prepared_rediscovery);
            let kind = rejection_revocation_kind(failure.kind);
            return Err(post_recovery(
                failure.assignment,
                failure.classic_generation,
                schedule,
                coordinator,
                kind,
            ));
        }
        prepared_rejoin.commit();
        if let Some(prepared) = prepared_rediscovery {
            prepared.commit();
        }
        return Ok(());
    }
    if entry.fetch.activation().is_none() && entry.fetch.machine_assignment_epoch().is_none() {
        match retire_and_revoke_classic_group_assignment(
            &entry.classic,
            &mut entry.catalog,
            &mut entry.processing_lease,
            &mut entry.fetch,
            assignment,
            generation,
        ) {
            Ok(_retirement) => {
                prepared_rejoin.commit();
                if let Some(prepared) = prepared_rediscovery {
                    prepared.commit();
                }
            }
            Err(failure) => {
                drop(prepared_rejoin);
                drop(prepared_rediscovery);
                let kind = rejection_revocation_kind(failure.kind);
                return Err(post_recovery(
                    failure.assignment,
                    failure.classic_generation,
                    schedule,
                    coordinator,
                    kind,
                ));
            }
        }
        return Ok(());
    }
    if let Err((error, assignment)) = stage_classic_group_revocation(
        &mut entry.catalog,
        &entry.fetch,
        &mut entry.revocation,
        assignment,
        generation,
        schedule.due(),
        now,
    ) {
        drop(prepared_rejoin);
        drop(prepared_rediscovery);
        return Err(post_recovery(
            assignment,
            generation,
            schedule,
            coordinator,
            GracefulRevocation(error),
        ));
    }
    prepared_rejoin.commit();
    if let Some(prepared) = prepared_rediscovery {
        prepared.commit();
    }
    Ok(())
}

pub(super) const fn rejection_revocation_kind(
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

fn post_recovery(
    assignment: LiveGroupAssignment,
    generation: ClassicGeneration,
    schedule: ClassicRejoinSchedule,
    coordinator: ClassicCoordinatorRecovery,
    failure: ClassicRejectionInstallFailure,
) -> ClassicRejectionPostCore {
    ClassicRejectionPostCore::heartbeat(
        assignment,
        generation,
        ClassicGroupEffect::ArmRejoin {
            schedule,
            coordinator,
        },
        failure,
    )
}

use ClassicRejectionInstallFailure::{
    Assignment, FetchRetirement, GracefulRevocation, MachineState, ProcessingLease,
    ProcessingLeaseCycleUnavailable, RediscoveryState, RejoinState,
};
