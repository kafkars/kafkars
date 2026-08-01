//! Prepared KIP-848 assignment installation after exact prior-owner retirement.

use kafka_client_core::{
    ClassicProcessingLeaseFence, ConsumerGroupMemberEpoch, LiveGroupAssignment, MembershipCycle,
    Moment,
};

use crate::clock::OperationDeadline;

use super::{
    classic_group_entry_fault::ClassicGroupEntryFault,
    classic_group_position::{
        ClassicGroupPositionExecutionState, ClassicGroupPositionPreparation,
        prepare_classic_group_position_with_policy,
    },
    consumer_group_execution::ConsumerGroupExecutionError,
    registry_entry::GroupConsumerEntry,
    session_catalog_consumer::ConsumerGroupMemberCandidate,
};

/// Exact new assignment retained while its previous owner drains.
#[must_use = "a prepared KIP-848 assignment must be installed or explicitly dropped"]
pub(super) struct PreparedConsumerGroupAssignmentInstall {
    candidate: ConsumerGroupMemberCandidate,
    cycle: MembershipCycle,
    member_epoch: ConsumerGroupMemberEpoch,
    assignment: LiveGroupAssignment,
    deadline: OperationDeadline,
    observed_at: Moment,
}

impl PreparedConsumerGroupAssignmentInstall {
    pub(super) const fn new(
        candidate: ConsumerGroupMemberCandidate,
        cycle: MembershipCycle,
        member_epoch: ConsumerGroupMemberEpoch,
        assignment: LiveGroupAssignment,
        deadline: OperationDeadline,
        observed_at: Moment,
    ) -> Self {
        Self {
            candidate,
            cycle,
            member_epoch,
            assignment,
            deadline,
            observed_at,
        }
    }

    pub(super) const fn member_id(&self) -> kafka_client_core::MemberId {
        self.candidate.member_id()
    }

    pub(super) const fn member_epoch(&self) -> ConsumerGroupMemberEpoch {
        self.member_epoch
    }

    pub(super) const fn assignment(&self) -> &LiveGroupAssignment {
        &self.assignment
    }

    pub(super) const fn refresh_resolution_boundary(
        mut self,
        deadline: OperationDeadline,
        observed_at: Moment,
    ) -> Self {
        self.deadline = deadline;
        self.observed_at = observed_at;
        self
    }
}

pub(super) fn install_consumer_group_assignment(
    entry: &mut GroupConsumerEntry,
    prepared: PreparedConsumerGroupAssignmentInstall,
) -> Result<(), ConsumerGroupExecutionError> {
    install(entry, prepared, false)
}

pub(super) fn install_reconciled_consumer_group_assignment(
    entry: &mut GroupConsumerEntry,
    prepared: PreparedConsumerGroupAssignmentInstall,
) -> Result<(), ConsumerGroupExecutionError> {
    install(entry, prepared, true)
}

fn install(
    entry: &mut GroupConsumerEntry,
    prepared: PreparedConsumerGroupAssignmentInstall,
    reconciled: bool,
) -> Result<(), ConsumerGroupExecutionError> {
    if entry.catalog.live_assignment().is_some()
        || !entry.position.is_dormant()
        || entry.processing_lease.active_schedule().is_some()
        || entry.processing_lease.pending_expiration().is_some()
        || entry.fetch.activation().is_some()
    {
        return Err(ConsumerGroupExecutionError::EffectShape);
    }
    let PreparedConsumerGroupAssignmentInstall {
        candidate,
        cycle,
        member_epoch,
        assignment,
        deadline,
        observed_at,
    } = prepared;
    let position = match prepare_classic_group_position_with_policy(
        &entry.catalog,
        cycle,
        &assignment,
        deadline,
        observed_at,
        entry.missing_offset_policy,
    ) {
        Ok(position) => position,
        Err(error) => {
            entry.fault = Some(ClassicGroupEntryFault::ConsumerGroupPositionPreparation {
                assignment,
                error,
            });
            return Err(ConsumerGroupExecutionError::EffectShape);
        }
    };
    let processing_fence = ClassicProcessingLeaseFence::new(
        entry.group_id(),
        cycle,
        assignment.assignment_generation(),
    );
    let processing = match entry
        .processing_lease
        .prepare_activation(processing_fence, observed_at)
    {
        Ok(processing) => processing,
        Err(error) => {
            drop(position);
            entry.fault = Some(
                ClassicGroupEntryFault::ConsumerGroupProcessingLeaseActivation {
                    assignment,
                    error,
                },
            );
            return Err(ConsumerGroupExecutionError::EffectShape);
        }
    };
    if reconciled {
        entry.catalog.commit_consumer_group_reconciliation_install(
            candidate,
            cycle,
            member_epoch,
            assignment,
        );
    } else {
        entry
            .catalog
            .commit_consumer_group_install(candidate, cycle, member_epoch, assignment);
    }
    entry.catalog.stage_installed_assignment_event();
    entry.catalog.confirm_sync_event();
    let _transition = processing.commit();
    entry.position.set(match position {
        ClassicGroupPositionPreparation::Prepared(prepared) => {
            ClassicGroupPositionExecutionState::Prepared(prepared)
        }
        ClassicGroupPositionPreparation::Complete(completed) => {
            ClassicGroupPositionExecutionState::Complete(completed)
        }
    });
    entry
        .consumer
        .as_mut()
        .ok_or(ConsumerGroupExecutionError::MissingPrepared)?
        .commit_reconcile_cycle(cycle);
    Ok(())
}
