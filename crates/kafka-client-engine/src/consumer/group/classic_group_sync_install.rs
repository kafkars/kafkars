//! Atomic catalog and Heartbeat installation from one normalized Sync assignment.

use kafka_client_core::{
    ClassicAssignmentReconciliation, ClassicGroupEffect, ClassicGroupInput, ClassicGroupPhase,
    ClassicGroupTransition, ClassicProcessingLeaseFence, LiveGroupAssignment, MembershipCycle,
    Moment,
};

use crate::{
    driver::classic_group::SyncGroupTerminal, protocol::consumer::NamedAssignmentPartition,
};

use super::{
    classic_group_assignment_decode::decode_classic_group_assignment,
    classic_group_entry_fault::ClassicGroupEntryFault,
    classic_group_execution::ClassicGroupExecutionError,
    classic_group_position::{
        ClassicGroupPositionExecutionState, ClassicGroupPositionPreparation,
        prepare_classic_group_position_with_policy,
    },
    classic_group_reconciliation::PreparedClassicGroupReconciliation,
    classic_group_sync_interpret::{
        SyncInterpretationFailure, failure, freeze_post_core, stage_confirmation,
    },
    registry_entry::GroupConsumerEntry,
};

const TICKS_PER_MILLISECOND: u64 = 1_000_000;

#[expect(
    clippy::result_large_err,
    reason = "the error retains the exact linear generated terminal without another allocation"
)]
#[allow(
    clippy::too_many_lines,
    reason = "the atomic Sync installation keeps every staged owner and commit point visible in one transition"
)]
pub(super) fn install_sync_assignment(
    entry: &mut GroupConsumerEntry,
    cycle: MembershipCycle,
    now: Moment,
    terminal: SyncGroupTerminal,
    partitions: Vec<NamedAssignmentPartition>,
) -> Result<(), SyncInterpretationFailure> {
    let (terminal, transition) = apply_sync_success(entry, cycle, now, terminal, partitions)?;
    let mut effects = transition.into_effects();
    let first = effects.next();
    let second = effects.next();
    let (assignment, generation, heartbeat) = match (first, second) {
        (None, None)
            if entry.classic.machine().phase() == ClassicGroupPhase::Lost
                && entry.classic.machine().live_assignment().is_none()
                && entry.catalog.live_assignment().is_none()
                && entry.heartbeat.is_dormant() =>
        {
            return stage_confirmation(entry, terminal);
        }
        (Some(ClassicGroupEffect::Reconcile { reconciliation }), None) => {
            return install_sync_reconciliation(entry, cycle, now, terminal, reconciliation);
        }
        (
            Some(ClassicGroupEffect::Install {
                assignment,
                classic_generation,
                heartbeat,
            }),
            None,
        ) if heartbeat.attempt().cycle() == cycle
            && heartbeat.attempt().assignment_generation()
                == assignment.assignment_generation() =>
        {
            (assignment, classic_generation, heartbeat)
        }
        _ => return freeze_post_core(entry, terminal, SyncTerminal),
    };
    let processing_fence = ClassicProcessingLeaseFence::new(
        entry.group_id(),
        cycle,
        assignment.assignment_generation(),
    );
    let heartbeat_install = match entry.heartbeat.prepare_install(heartbeat) {
        Ok(prepared) => prepared,
        Err(_error) => return freeze_post_core(entry, terminal, SyncTerminal),
    };
    let position_install = match prepare_classic_group_position_with_policy(
        &entry.catalog,
        cycle,
        &assignment,
        terminal.key().deadline(),
        now,
        entry.missing_offset_policy,
    ) {
        Ok(prepared) => prepared,
        Err(error) => {
            drop(heartbeat_install);
            entry.fault = Some(ClassicGroupEntryFault::SyncPositionPreparation { terminal, error });
            return Err(SyncInterpretationFailure::post_core(
                ClassicGroupExecutionError::PositionPreparation,
            ));
        }
    };
    let processing_install = match entry
        .processing_lease
        .prepare_activation(processing_fence, now)
    {
        Ok(prepared) => prepared,
        Err(error) => {
            drop(heartbeat_install);
            entry.fault = Some(ClassicGroupEntryFault::SyncProcessingLeaseActivation {
                assignment,
                generation,
                terminal,
                error,
            });
            return Err(SyncInterpretationFailure::post_core(
                ClassicGroupExecutionError::ProcessingLease(error),
            ));
        }
    };
    let catalog_install =
        match entry
            .classic
            .prepare_install(&mut entry.catalog, assignment, generation)
        {
            Ok(prepared) => prepared,
            Err(install) => {
                drop(processing_install);
                drop(heartbeat_install);
                let kind = install.kind;
                entry.fault = Some(ClassicGroupEntryFault::SyncInstall {
                    failure: install,
                    generation,
                    terminal,
                });
                return Err(SyncInterpretationFailure::post_core(
                    ClassicGroupExecutionError::Assignment(kind),
                ));
            }
        };
    catalog_install.commit();
    entry.catalog.stage_installed_assignment_event();
    heartbeat_install.commit();
    let _transition = processing_install.commit();
    install_position(entry, position_install);
    stage_confirmation(entry, terminal)
}

fn install_position(entry: &mut GroupConsumerEntry, position: ClassicGroupPositionPreparation) {
    entry.position.set(match position {
        ClassicGroupPositionPreparation::Prepared(prepared) => {
            ClassicGroupPositionExecutionState::Prepared(prepared)
        }
        ClassicGroupPositionPreparation::Complete(completed) => {
            ClassicGroupPositionExecutionState::Complete(completed)
        }
    });
}

#[expect(
    clippy::result_large_err,
    reason = "post-core Sync failure retains the exact terminal and reconciliation ownership"
)]
fn install_sync_reconciliation(
    entry: &mut GroupConsumerEntry,
    cycle: MembershipCycle,
    now: Moment,
    terminal: SyncGroupTerminal,
    reconciliation: ClassicAssignmentReconciliation,
) -> Result<(), SyncInterpretationFailure> {
    if entry.classic_reconciliation.is_some()
        || !entry.position.is_dormant()
        || !entry.revocation.is_dormant()
        || reconciliation.replacement_cycle() != cycle
        || entry.classic.machine().phase() != ClassicGroupPhase::Reconciling
        || entry.classic.machine().live_assignment()
            != Some(reconciliation.replacement_assignment())
    {
        return freeze_post_core(entry, terminal, SyncTerminal);
    }
    let Some(rebalance_timeout_ticks) =
        u64::try_from(entry.classic.machine().timing().rebalance_timeout_ms())
            .ok()
            .and_then(|milliseconds| milliseconds.checked_mul(TICKS_PER_MILLISECOND))
    else {
        return freeze_post_core(entry, terminal, SyncTerminal);
    };
    let Some(revocation_deadline) = now.checked_deadline_after(rebalance_timeout_ticks) else {
        return freeze_post_core(entry, terminal, SyncTerminal);
    };
    let Ok(added_assignment) = copy_added_assignment(&reconciliation) else {
        return freeze_post_core(entry, terminal, SyncTerminal);
    };
    let Ok(revocation_assignment) = copy_assignment(reconciliation.previous_assignment()) else {
        return freeze_post_core(entry, terminal, SyncTerminal);
    };
    let position = match prepare_classic_group_position_with_policy(
        &entry.catalog,
        reconciliation.replacement_cycle(),
        &added_assignment,
        terminal.key().deadline(),
        now,
        entry.missing_offset_policy,
    ) {
        Ok(position) => position,
        Err(error) => {
            entry.fault = Some(ClassicGroupEntryFault::SyncPositionPreparation { terminal, error });
            return Err(SyncInterpretationFailure::post_core(
                ClassicGroupExecutionError::PositionPreparation,
            ));
        }
    };
    let heartbeat = match entry.heartbeat.prepare_install(reconciliation.heartbeat()) {
        Ok(heartbeat) => heartbeat,
        Err(_error) => return freeze_post_core(entry, terminal, SyncTerminal),
    };
    let Some(candidate) = entry.classic.pending() else {
        drop(heartbeat);
        return freeze_post_core(entry, terminal, SyncTerminal);
    };
    let Some(epoch) = entry.catalog.prepare_classic_reconciliation_epoch(
        candidate,
        reconciliation.previous_assignment(),
        reconciliation.previous_classic_generation(),
        reconciliation.replacement_classic_generation(),
    ) else {
        drop(heartbeat);
        return freeze_post_core(entry, terminal, SyncTerminal);
    };
    epoch.commit();
    heartbeat.commit();
    entry.classic_reconciliation = Some(PreparedClassicGroupReconciliation::new(
        reconciliation,
        revocation_assignment,
        position,
        revocation_deadline,
    ));
    stage_confirmation(entry, terminal)
}

fn copy_assignment(assignment: &LiveGroupAssignment) -> Result<LiveGroupAssignment, ()> {
    let mut partitions = Vec::new();
    partitions
        .try_reserve_exact(assignment.partitions().len())
        .map_err(|_error| ())?;
    partitions.extend_from_slice(assignment.partitions());
    LiveGroupAssignment::try_new(
        assignment.group_id(),
        assignment.member_id(),
        assignment.assignment_generation(),
        partitions,
    )
    .map_err(|_error| ())
}

fn copy_added_assignment(
    reconciliation: &ClassicAssignmentReconciliation,
) -> Result<LiveGroupAssignment, ()> {
    let replacement = reconciliation.replacement_assignment();
    let mut added = Vec::new();
    added
        .try_reserve_exact(reconciliation.delta().added().len())
        .map_err(|_error| ())?;
    added.extend_from_slice(reconciliation.delta().added());
    LiveGroupAssignment::try_new(
        replacement.group_id(),
        replacement.member_id(),
        replacement.assignment_generation(),
        added,
    )
    .map_err(|_error| ())
}

#[expect(
    clippy::result_large_err,
    reason = "the error retains the exact linear generated terminal without another allocation"
)]
fn apply_sync_success(
    entry: &mut GroupConsumerEntry,
    cycle: MembershipCycle,
    now: Moment,
    terminal: SyncGroupTerminal,
    partitions: Vec<NamedAssignmentPartition>,
) -> Result<(SyncGroupTerminal, ClassicGroupTransition), SyncInterpretationFailure> {
    let Some(candidate) = entry.classic.pending() else {
        return Err(failure(terminal, SyncTerminal));
    };
    if !entry.heartbeat.is_dormant() {
        return Err(failure(terminal, SyncTerminal));
    }
    if !entry.position.is_dormant() {
        return Err(failure(terminal, SyncTerminal));
    }
    let partitions = match decode_classic_group_assignment(&entry.catalog, candidate, partitions) {
        Ok(partitions) => partitions,
        Err(_error) => return Err(failure(terminal, SyncTerminal)),
    };
    let transition = match entry.classic.apply(ClassicGroupInput::SyncSucceeded {
        cycle,
        now,
        partitions,
    }) {
        Ok(transition) => transition,
        Err(error) => {
            return Err(failure(
                terminal,
                ClassicGroupExecutionError::Core(error.kind()),
            ));
        }
    };
    Ok((terminal, transition))
}

use ClassicGroupExecutionError::SyncTerminal;
