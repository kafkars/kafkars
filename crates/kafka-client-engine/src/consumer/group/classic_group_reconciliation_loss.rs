//! Deferred cooperative-reconciliation loss across revocation and position ownership.

use kafka_client_core::{ClassicGeneration, ClassicGroupPhase, LiveGroupAssignment, Moment};

use super::{
    classic_group_assignment::{
        ClassicGroupAssignmentPreparationFailureKind, ClassicGroupReconciliationRevocationError,
        ClassicGroupRevocationFailure, ClassicGroupRevocationFailureKind,
        retire_lost_classic_group_reconciliation,
    },
    classic_group_execution::ClassicGroupExecutionError,
    classic_group_position::{
        ClassicGroupPositionCloseTurn, ClassicGroupPositionExecutionState,
        ClassicGroupPositionPreparation, close_entry_position,
    },
    registry::GroupConsumerRegistry,
    registry_entry::GroupConsumerEntry,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ClassicGroupReconciliationLossTurn {
    Idle,
    Progress,
    Blocked,
}

#[expect(
    clippy::result_large_err,
    reason = "a rejected loss transition returns the exact assignment and generation ownership without boxing or reconstruction"
)]
pub(super) fn stage_classic_group_reconciliation_loss(
    entry: &mut GroupConsumerEntry,
    assignment: LiveGroupAssignment,
    generation: ClassicGeneration,
) -> Result<(), ClassicGroupRevocationFailure> {
    let Some(pending) = entry.classic_reconciliation.as_ref() else {
        return Err(catalog_failure(
            ClassicGroupAssignmentPreparationFailureKind::AssignmentMismatch,
            assignment,
            generation,
        ));
    };
    let reconciliation = pending.reconciliation();
    let previous = reconciliation.previous_assignment();
    let shape = if pending.assignment_loss_is_staged()
        || reconciliation.replacement_assignment() != &assignment
    {
        Err(ClassicGroupAssignmentPreparationFailureKind::AssignmentMismatch)
    } else if reconciliation.replacement_classic_generation() != generation
        || entry.catalog.classic_generation() != Some(generation.get())
    {
        Err(ClassicGroupAssignmentPreparationFailureKind::GenerationMismatch)
    } else if !matches!(
        entry.classic.machine().phase(),
        ClassicGroupPhase::Lost | ClassicGroupPhase::Fatal
    ) || entry.classic.machine().live_assignment().is_some()
        || entry.classic.machine().live_cycle().is_some()
        || entry.classic.machine().live_generation().is_some()
    {
        Err(ClassicGroupAssignmentPreparationFailureKind::MachinePhase)
    } else if entry.catalog.live_assignment() != Some(previous) {
        Err(ClassicGroupAssignmentPreparationFailureKind::AssignmentMismatch)
    } else if entry.catalog.membership_cycle() != Some(reconciliation.previous_cycle()) {
        Err(ClassicGroupAssignmentPreparationFailureKind::CatalogChanged)
    } else {
        Ok(previous.assignment_generation().get())
    };
    let epoch = match shape {
        Ok(epoch) => epoch,
        Err(kind) => return Err(catalog_failure(kind, assignment, generation)),
    };
    entry
        .classic_reconciliation
        .as_mut()
        .unwrap_or_else(|| unreachable!("validated reconciliation remains installed"))
        .stage_assignment_loss(assignment, generation)
        .map_err(|(assignment, generation)| {
            catalog_failure(
                ClassicGroupAssignmentPreparationFailureKind::AssignmentMismatch,
                assignment,
                generation,
            )
        })?;
    entry.catalog.lose_consumer_group_graceful_revocation(epoch);
    Ok(())
}

impl GroupConsumerRegistry {
    pub(super) fn turn_one_classic_group_reconciliation_loss(
        &mut self,
        now: Moment,
    ) -> Result<ClassicGroupReconciliationLossTurn, ClassicGroupExecutionError> {
        let Some(index) = self.entries.iter().position(|entry| {
            entry
                .classic_reconciliation
                .as_ref()
                .is_some_and(
                    super::classic_group_reconciliation::PreparedClassicGroupReconciliation::assignment_loss_is_staged,
                )
        }) else {
            return Ok(ClassicGroupReconciliationLossTurn::Idle);
        };
        let entry = &mut self.entries[index];
        if entry.revocation.active_assignment_epoch().is_some() {
            if !entry.revocation.pending_is_classic_reconciliation() {
                return Err(ClassicGroupExecutionError::Reconciliation);
            }
            entry
                .revocation
                .lose_owner()
                .map_err(|_error| ClassicGroupExecutionError::Reconciliation)?;
            return Ok(ClassicGroupReconciliationLossTurn::Progress);
        }
        if !entry.revocation.is_dormant() || !entry.heartbeat.is_dormant() {
            return Ok(ClassicGroupReconciliationLossTurn::Blocked);
        }
        if transfer_reconciliation_position_for_loss(entry)? {
            return Ok(ClassicGroupReconciliationLossTurn::Progress);
        }
        match close_entry_position(entry, now)? {
            ClassicGroupPositionCloseTurn::Progress => {
                return Ok(ClassicGroupReconciliationLossTurn::Progress);
            }
            ClassicGroupPositionCloseTurn::Blocked => {
                return Ok(ClassicGroupReconciliationLossTurn::Blocked);
            }
            ClassicGroupPositionCloseTurn::Idle => {}
        }
        let pending = entry
            .classic_reconciliation
            .as_ref()
            .ok_or(ClassicGroupExecutionError::Reconciliation)?;
        let reconciliation = pending.reconciliation();
        let Some((lost, generation)) = pending.assignment_loss() else {
            return Err(ClassicGroupExecutionError::Reconciliation);
        };
        if lost != reconciliation.replacement_assignment()
            || generation != reconciliation.replacement_classic_generation()
        {
            return Err(ClassicGroupExecutionError::Reconciliation);
        }
        let previous_cycle = reconciliation.previous_cycle();
        let previous = reconciliation.previous_assignment();
        if let Err(error) = retire_lost_classic_group_reconciliation(
            &entry.classic,
            &mut entry.catalog,
            &mut entry.processing_lease,
            &mut entry.fetch,
            previous,
            previous_cycle,
            generation,
        ) {
            let mapped = map_retirement(error);
            return Err(mapped);
        }
        drop(entry.classic_reconciliation.take());
        Ok(ClassicGroupReconciliationLossTurn::Progress)
    }
}

fn transfer_reconciliation_position_for_loss(
    entry: &mut GroupConsumerEntry,
) -> Result<bool, ClassicGroupExecutionError> {
    let pending = entry
        .classic_reconciliation
        .as_mut()
        .ok_or(ClassicGroupExecutionError::Reconciliation)?;
    if pending.position_was_installed() {
        return Ok(false);
    }
    if !entry.position.is_dormant() {
        return Err(ClassicGroupExecutionError::PositionPending);
    }
    let position = pending
        .take_position()
        .ok_or(ClassicGroupExecutionError::PositionPending)?;
    entry.position.set(match position {
        ClassicGroupPositionPreparation::Prepared(prepared) => {
            ClassicGroupPositionExecutionState::Prepared(prepared)
        }
        ClassicGroupPositionPreparation::Complete(completed) => {
            ClassicGroupPositionExecutionState::Complete(completed)
        }
    });
    Ok(true)
}

fn catalog_failure(
    kind: ClassicGroupAssignmentPreparationFailureKind,
    assignment: LiveGroupAssignment,
    classic_generation: ClassicGeneration,
) -> ClassicGroupRevocationFailure {
    ClassicGroupRevocationFailure {
        kind: ClassicGroupRevocationFailureKind::Catalog(kind),
        assignment,
        classic_generation,
    }
}

const fn map_retirement(
    error: ClassicGroupReconciliationRevocationError,
) -> ClassicGroupExecutionError {
    match error {
        ClassicGroupReconciliationRevocationError::Catalog(kind) => {
            ClassicGroupExecutionError::Assignment(kind)
        }
        ClassicGroupReconciliationRevocationError::ProcessingLease(error) => {
            ClassicGroupExecutionError::ProcessingLease(error)
        }
        ClassicGroupReconciliationRevocationError::Fetch(error) => {
            ClassicGroupExecutionError::FetchRetirement(error)
        }
    }
}
