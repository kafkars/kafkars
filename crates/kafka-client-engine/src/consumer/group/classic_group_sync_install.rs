//! Atomic catalog and Heartbeat installation from one normalized Sync assignment.

use kafka_client_core::{
    ClassicGroupEffect, ClassicGroupInput, ClassicGroupPhase, MembershipCycle, Moment,
};

use crate::{
    driver::classic_group::SyncGroupTerminal, protocol::consumer::NamedAssignmentPartition,
};

use super::{
    classic_group_assignment_decode::decode_classic_group_assignment,
    classic_group_entry_fault::ClassicGroupEntryFault,
    classic_group_execution::ClassicGroupExecutionError,
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
    let Some(candidate) = entry.classic.pending() else {
        return Err(failure(terminal, SyncTerminal));
    };
    if !entry.heartbeat.is_dormant() {
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
    let heartbeat_install = match entry.heartbeat.prepare_install(heartbeat) {
        Ok(prepared) => prepared,
        Err(_error) => return freeze_post_core(entry, terminal, SyncTerminal),
    };
    let catalog_install =
        match entry
            .classic
            .prepare_install(&mut entry.catalog, assignment, generation)
        {
            Ok(prepared) => prepared,
            Err(install) => {
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
    heartbeat_install.commit();
    stage_confirmation(entry, terminal)
}

use ClassicGroupExecutionError::SyncTerminal;
