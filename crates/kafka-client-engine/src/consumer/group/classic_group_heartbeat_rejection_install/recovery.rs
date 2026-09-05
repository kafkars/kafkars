//! Atomic revocation and bounded recovery installation after Heartbeat loss.

use kafka_client_core::{
    ClassicCoordinatorRecovery, ClassicGeneration, ClassicGroupEffect, ClassicGroupPhase,
    ClassicRejoinSchedule, LiveGroupAssignment, Moment,
};

use super::super::{
    classic_group_assignment::{
        retire_and_revoke_classic_group_assignment, ClassicGroupRevocationFailureKind,
    },
    classic_group_reconciliation_loss::stage_classic_group_reconciliation_loss,
    classic_group_rediscovery::{
        ClassicCoordinatorRediscovery, PreparedClassicCoordinatorRediscovery,
    },
    classic_group_rejection_fault::{ClassicRejectionInstallFailure, ClassicRejectionPostCore},
    classic_group_rejoin::{ClassicGroupRejoinExecution, PreparedClassicRejoinInstall},
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
    let Some(revocation_deadline) = classic_group_revocation_deadline(entry, now) else {
        return Err(post_recovery(
            assignment,
            generation,
            schedule,
            coordinator,
            MachineState,
        ));
    };
    let (prepared_rediscovery, prepared_rejoin) = match prepare_recovery_install(
        &mut entry.rediscovery,
        &mut entry.rejoin,
        schedule,
        coordinator,
    ) {
        Ok(prepared) => prepared,
        Err(failure) => {
            return Err(post_recovery(
                assignment,
                generation,
                schedule,
                coordinator,
                failure,
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
            let kind = rejection_revocation_kind(failure.kind);
            return Err(post_recovery(
                failure.assignment,
                failure.classic_generation,
                schedule,
                coordinator,
                kind,
            ));
        }
        commit_recovery_install(prepared_rediscovery, prepared_rejoin);
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
                commit_recovery_install(prepared_rediscovery, prepared_rejoin);
            }
            Err(failure) => {
                drop(prepared_rejoin);
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
        revocation_deadline,
        now,
    ) {
        drop(prepared_rejoin);
        return Err(post_recovery(
            assignment,
            generation,
            schedule,
            coordinator,
            GracefulRevocation(error),
        ));
    }
    commit_recovery_install(prepared_rediscovery, prepared_rejoin);
    Ok(())
}

fn prepare_recovery_install<'a>(
    rediscovery: &'a mut ClassicCoordinatorRediscovery,
    rejoin: &'a mut ClassicGroupRejoinExecution,
    schedule: ClassicRejoinSchedule,
    coordinator: ClassicCoordinatorRecovery,
) -> Result<
    (
        Option<PreparedClassicCoordinatorRediscovery<'a>>,
        PreparedClassicRejoinInstall<'a>,
    ),
    ClassicRejectionInstallFailure,
> {
    let prepared_rediscovery = match coordinator {
        ClassicCoordinatorRecovery::Retain => None,
        ClassicCoordinatorRecovery::Rediscover => Some(
            rediscovery
                .prepare_rediscovery_install()
                .map_err(|_error| RediscoveryState)?,
        ),
    };
    let prepared_rejoin = rejoin
        .prepare_rejoin_install(schedule)
        .map_err(|_error| RejoinState)?;
    Ok((prepared_rediscovery, prepared_rejoin))
}

fn commit_recovery_install(
    prepared_rediscovery: Option<PreparedClassicCoordinatorRediscovery<'_>>,
    prepared_rejoin: PreparedClassicRejoinInstall<'_>,
) {
    prepared_rejoin.commit();
    if let Some(prepared) = prepared_rediscovery {
        prepared.commit();
    }
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

fn classic_group_revocation_deadline(
    entry: &GroupConsumerEntry,
    now: Moment,
) -> Option<kafka_client_core::Deadline> {
    let ticks = u64::try_from(entry.classic.machine().timing().rebalance_timeout_ms())
        .ok()?
        .checked_mul(TICKS_PER_MILLISECOND)?;
    now.checked_deadline_after(ticks)
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

const TICKS_PER_MILLISECOND: u64 = 1_000_000;
