//! Atomic catalog and Heartbeat installation from one normalized Sync assignment.

use kafka_client_core::{
    ClassicGroupEffect, ClassicGroupInput, ClassicGroupPhase, ClassicGroupTransition,
    ClassicProcessingLeaseFence, MembershipCycle, Moment,
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
    classic_group_sync_interpret::{
        SyncInterpretationFailure, failure, freeze_post_core, stage_confirmation,
    },
    registry_entry::GroupConsumerEntry,
};

#[expect(
    clippy::result_large_err,
    reason = "the error retains the exact linear generated terminal without another allocation"
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
    if first.is_none()
        && effects.next().is_none()
        && entry.classic.machine().phase() == ClassicGroupPhase::Lost
        && entry.classic.machine().live_assignment().is_none()
        && entry.catalog.live_assignment().is_none()
        && entry.heartbeat.is_dormant()
    {
        return stage_confirmation(entry, terminal);
    }
    let (assignment, generation, heartbeat) = match first {
        Some(ClassicGroupEffect::Install {
            assignment,
            classic_generation,
            heartbeat,
        }) if effects.next().is_none()
            && heartbeat.attempt().cycle() == cycle
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
