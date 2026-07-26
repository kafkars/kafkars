//! Normalized Sync terminal dispatch, deadline settlement, and restoration.

use kafka_client_core::{ClassicGroupInput, Moment};

use crate::{
    driver::classic_group::SyncGroupTerminal,
    protocol::consumer::{ClassicSyncOutcome, normalize_classic_sync_response},
};

use super::{
    classic_group_entry_fault::ClassicGroupEntryFault,
    classic_group_execution::ClassicGroupExecutionError,
    classic_group_sync_install::install_sync_assignment,
    classic_group_sync_rejection::{ClassicSyncRejectionFailure, apply_sync_rejection},
    registry_entry::GroupConsumerEntry,
};

pub(super) struct SyncInterpretationFailure {
    sync_failure_kind: ClassicGroupExecutionError,
    restorable_sync_terminal: Option<SyncGroupTerminal>,
}

impl SyncInterpretationFailure {
    pub(super) fn into_parts(self) -> (ClassicGroupExecutionError, Option<SyncGroupTerminal>) {
        (self.sync_failure_kind, self.restorable_sync_terminal)
    }

    pub(super) const fn post_core(sync_failure_kind: ClassicGroupExecutionError) -> Self {
        Self {
            sync_failure_kind,
            restorable_sync_terminal: None,
        }
    }
}

#[expect(
    clippy::result_large_err,
    reason = "failure returns the exact linear generated terminal when restoration is possible"
)]
pub(super) fn interpret_sync(
    entry: &mut GroupConsumerEntry,
    now: Moment,
    terminal: SyncGroupTerminal,
) -> Result<(), SyncInterpretationFailure> {
    let cycle = terminal.key().cycle();
    if terminal.key().deadline().core().is_elapsed_at(now) {
        return apply_or_freeze(
            entry,
            terminal,
            ClassicGroupInput::DeadlineElapsed { cycle, now },
        );
    }
    let outcome = match normalize_terminal(&terminal) {
        Ok(outcome) => outcome,
        Err(kind) => return Err(failure(terminal, kind)),
    };
    let partitions = match outcome {
        Some(ClassicSyncOutcome::Rejected(rejection)) => {
            return match apply_sync_rejection(entry, cycle, now, rejection) {
                Ok(()) => stage_confirmation(entry, terminal),
                Err(ClassicSyncRejectionFailure::Restorable(kind)) => Err(failure(terminal, kind)),
                Err(ClassicSyncRejectionFailure::PostCore(rejection)) => {
                    entry.fault = Some(ClassicGroupEntryFault::SyncRejectionPostCore {
                        rejection,
                        terminal,
                    });
                    Err(SyncInterpretationFailure {
                        sync_failure_kind: ClassicGroupExecutionError::RejoinPostCore,
                        restorable_sync_terminal: None,
                    })
                }
            };
        }
        Some(ClassicSyncOutcome::Assigned { partitions, .. }) => partitions,
        None => {
            return apply_or_freeze(entry, terminal, ClassicGroupInput::SyncFailed { cycle });
        }
    };
    install_sync_assignment(entry, cycle, now, terminal, partitions)
}

fn normalize_terminal(
    terminal: &SyncGroupTerminal,
) -> Result<Option<ClassicSyncOutcome>, ClassicGroupExecutionError> {
    let Ok(response) = terminal.result() else {
        return Ok(None);
    };
    let version = terminal.selected_version().ok_or(SyncTerminal)?;
    normalize_classic_sync_response(version, response)
        .map(Some)
        .map_err(|_error| SyncTerminal)
}

#[expect(
    clippy::result_large_err,
    reason = "failure returns the exact linear generated terminal when restoration is possible"
)]
fn apply_or_freeze(
    entry: &mut GroupConsumerEntry,
    terminal: SyncGroupTerminal,
    input: ClassicGroupInput,
) -> Result<(), SyncInterpretationFailure> {
    let transition = match entry.classic.apply(input) {
        Ok(transition) => transition,
        Err(error) => {
            return Err(failure(
                terminal,
                ClassicGroupExecutionError::Core(error.kind()),
            ));
        }
    };
    if transition.into_effects().next().is_some() {
        return freeze_post_core(entry, terminal, SyncTerminal);
    }
    stage_confirmation(entry, terminal)
}

#[expect(
    clippy::result_large_err,
    reason = "failure returns the exact linear generated terminal when restoration is possible"
)]
pub(super) fn stage_confirmation(
    entry: &mut GroupConsumerEntry,
    terminal: SyncGroupTerminal,
) -> Result<(), SyncInterpretationFailure> {
    match entry.execution.stage_sync_confirmation() {
        Ok(()) => Ok(()),
        Err(kind) => {
            entry.fault = Some(ClassicGroupEntryFault::SyncConfirmationTerminal(terminal));
            Err(SyncInterpretationFailure {
                sync_failure_kind: kind,
                restorable_sync_terminal: None,
            })
        }
    }
}

#[expect(
    clippy::result_large_err,
    reason = "failure retains the exact linear generated terminal in the entry fault"
)]
pub(super) fn freeze_post_core(
    entry: &mut GroupConsumerEntry,
    terminal: SyncGroupTerminal,
    kind: ClassicGroupExecutionError,
) -> Result<(), SyncInterpretationFailure> {
    entry.fault = Some(ClassicGroupEntryFault::SyncPostCore(terminal));
    Err(SyncInterpretationFailure {
        sync_failure_kind: kind,
        restorable_sync_terminal: None,
    })
}

pub(super) fn failure(
    terminal: SyncGroupTerminal,
    kind: ClassicGroupExecutionError,
) -> SyncInterpretationFailure {
    SyncInterpretationFailure {
        sync_failure_kind: kind,
        restorable_sync_terminal: Some(terminal),
    }
}

use ClassicGroupExecutionError::SyncTerminal;
