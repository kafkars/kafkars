//! Exact preparation and infallible commit of core Install and Revoke effects.

use kafka_client_core::{
    ClassicGeneration, ClassicGroupPhase, ClassicProcessingLease, ClassicProcessingLeaseError,
    LiveGroupAssignment, PartitionIndex,
};

use super::{
    classic_group_fetch::{
        ClassicGroupFetchOwner, ClassicGroupFetchRetirement, ClassicGroupFetchRetirementError,
    },
    classic_group_owner::ClassicGroupOwner,
    session_catalog::GroupSessionCatalog,
};

#[must_use = "a prepared classic-group install must be committed"]
pub(super) struct PreparedClassicGroupInstall<'a> {
    owner: &'a mut ClassicGroupOwner,
    catalog: &'a mut GroupSessionCatalog,
    assignment: LiveGroupAssignment,
    classic_generation: ClassicGeneration,
}

#[must_use = "a prepared classic-group revoke must be committed"]
pub(super) struct PreparedClassicGroupRevoke<'a> {
    _owner: &'a ClassicGroupOwner,
    catalog: &'a mut GroupSessionCatalog,
    assignment: LiveGroupAssignment,
    classic_generation: ClassicGeneration,
}

#[must_use = "failed effect preparation retains the linear assignment"]
pub(super) struct ClassicGroupAssignmentPreparationFailure {
    pub(super) kind: ClassicGroupAssignmentPreparationFailureKind,
    pub(super) assignment: LiveGroupAssignment,
}

#[must_use = "failed classic-group revocation retains the exact assignment and generation"]
pub(super) struct ClassicGroupRevocationFailure {
    pub(super) kind: ClassicGroupRevocationFailureKind,
    pub(super) assignment: LiveGroupAssignment,
    pub(super) classic_generation: ClassicGeneration,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ClassicGroupAssignmentPreparationFailureKind {
    MissingCandidate,
    CatalogChanged,
    CatalogAlreadyAssigned,
    CatalogNotAssigned,
    MachinePhase,
    GroupMismatch,
    MemberMismatch,
    GenerationMismatch,
    AssignmentMismatch,
    UnsubscribedTopic,
    PartitionOutOfRange(PartitionIndex),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ClassicGroupRevocationFailureKind {
    Catalog(ClassicGroupAssignmentPreparationFailureKind),
    ProcessingLeaseCycleUnavailable,
    ProcessingLease(ClassicProcessingLeaseError),
    Fetch(ClassicGroupFetchRetirementError),
}

impl ClassicGroupOwner {
    pub(super) fn prepare_install<'a>(
        &'a mut self,
        catalog: &'a mut GroupSessionCatalog,
        assignment: LiveGroupAssignment,
        classic_generation: ClassicGeneration,
    ) -> Result<PreparedClassicGroupInstall<'a>, ClassicGroupAssignmentPreparationFailure> {
        let Some(candidate) = self.pending.as_ref() else {
            return Err(failure(MissingCandidate, assignment));
        };
        if !candidate.matches_catalog_base(catalog) {
            return Err(failure(CatalogChanged, assignment));
        }
        if catalog.live_assignment().is_some() {
            return Err(failure(CatalogAlreadyAssigned, assignment));
        }
        if self.machine().phase() != ClassicGroupPhase::Stable {
            return Err(failure(MachinePhase, assignment));
        }
        if assignment.group_id() != catalog.group_id() {
            return Err(failure(GroupMismatch, assignment));
        }
        if assignment.member_id() != candidate.local_member_id() {
            return Err(failure(MemberMismatch, assignment));
        }
        if self.machine().live_generation() != Some(classic_generation) {
            return Err(failure(GenerationMismatch, assignment));
        }
        if self.machine().live_assignment() != Some(&assignment) {
            return Err(failure(AssignmentMismatch, assignment));
        }
        if assignment
            .partitions()
            .iter()
            .map(|partition| partition.topic_id())
            .any(|topic_id| !candidate.local_owns_topic(topic_id))
        {
            return Err(failure(UnsubscribedTopic, assignment));
        }
        if let Some(partition) = assignment
            .partitions()
            .iter()
            .map(|partition| partition.partition())
            .find(|partition| i32::try_from(partition.get()).is_err())
        {
            return Err(failure(PartitionOutOfRange(partition), assignment));
        }
        Ok(PreparedClassicGroupInstall {
            owner: self,
            catalog,
            assignment,
            classic_generation,
        })
    }

    pub(super) fn prepare_revoke<'a>(
        &'a self,
        catalog: &'a mut GroupSessionCatalog,
        assignment: LiveGroupAssignment,
        classic_generation: ClassicGeneration,
    ) -> Result<PreparedClassicGroupRevoke<'a>, ClassicGroupAssignmentPreparationFailure> {
        let Some(current) = catalog.live_assignment() else {
            return Err(failure(CatalogNotAssigned, assignment));
        };
        if current != &assignment {
            return Err(failure(AssignmentMismatch, assignment));
        }
        if catalog.classic_generation() != Some(classic_generation.get()) {
            return Err(failure(GenerationMismatch, assignment));
        }
        if self.machine().group_id() != catalog.group_id() {
            return Err(failure(GroupMismatch, assignment));
        }
        if !matches!(
            self.machine().phase(),
            ClassicGroupPhase::WaitingToRejoin
                | ClassicGroupPhase::Lost
                | ClassicGroupPhase::Fatal
                | ClassicGroupPhase::Closed
        ) {
            return Err(failure(MachinePhase, assignment));
        }
        Ok(PreparedClassicGroupRevoke {
            _owner: self,
            catalog,
            assignment,
            classic_generation,
        })
    }
}

#[expect(
    clippy::result_large_err,
    reason = "rejection returns the exact linear assignment and generation without heap indirection"
)]
pub(super) fn retire_and_revoke_classic_group_assignment(
    owner: &ClassicGroupOwner,
    catalog: &mut GroupSessionCatalog,
    processing_lease: &mut ClassicProcessingLease,
    fetch: &mut ClassicGroupFetchOwner,
    assignment: LiveGroupAssignment,
    classic_generation: ClassicGeneration,
) -> Result<ClassicGroupFetchRetirement, ClassicGroupRevocationFailure> {
    let processing_fence = processing_lease
        .active_schedule()
        .map(kafka_client_core::ClassicProcessingLeaseSchedule::fence)
        .or_else(|| {
            processing_lease
                .pending_expiration()
                .map(|expiration| expiration.schedule().fence())
        });
    let prepared = owner
        .prepare_revoke(catalog, assignment, classic_generation)
        .map_err(|failure| ClassicGroupRevocationFailure {
            kind: ClassicGroupRevocationFailureKind::Catalog(failure.kind),
            assignment: failure.assignment,
            classic_generation,
        })?;
    let Some(retained_processing_fence) = processing_fence else {
        return Err(ClassicGroupRevocationFailure {
            kind: ClassicGroupRevocationFailureKind::ProcessingLeaseCycleUnavailable,
            assignment: prepared.assignment,
            classic_generation: prepared.classic_generation,
        });
    };
    let processing_fence = kafka_client_core::ClassicProcessingLeaseFence::new(
        prepared.assignment.group_id(),
        retained_processing_fence.cycle(),
        prepared.assignment.assignment_generation(),
    );
    let processing_revocation = match processing_lease.prepare_revocation(processing_fence) {
        Ok(prepared_revocation) => prepared_revocation,
        Err(error) => {
            return Err(ClassicGroupRevocationFailure {
                kind: ClassicGroupRevocationFailureKind::ProcessingLease(error),
                assignment: prepared.assignment,
                classic_generation: prepared.classic_generation,
            });
        }
    };
    let retirement = match fetch.retire_for_assignment_loss(&prepared.assignment) {
        Ok(retirement) => retirement,
        Err(error) => {
            return Err(ClassicGroupRevocationFailure {
                kind: ClassicGroupRevocationFailureKind::Fetch(error),
                assignment: prepared.assignment,
                classic_generation: prepared.classic_generation,
            });
        }
    };
    let _transition = processing_revocation.commit();
    prepared.commit();
    Ok(retirement)
}

impl PreparedClassicGroupInstall<'_> {
    pub(super) fn commit(self) {
        if let Some(candidate) = self.owner.pending.take() {
            self.catalog.commit_classic_group_install(
                candidate,
                self.assignment,
                self.classic_generation,
            );
        }
    }
}

impl PreparedClassicGroupRevoke<'_> {
    pub(super) fn commit(self) {
        self.catalog
            .commit_classic_group_revoke(self.assignment, self.classic_generation);
    }
}

fn failure(
    kind: ClassicGroupAssignmentPreparationFailureKind,
    assignment: LiveGroupAssignment,
) -> ClassicGroupAssignmentPreparationFailure {
    ClassicGroupAssignmentPreparationFailure { kind, assignment }
}

use ClassicGroupAssignmentPreparationFailureKind::{
    AssignmentMismatch, CatalogAlreadyAssigned, CatalogChanged, CatalogNotAssigned,
    GenerationMismatch, GroupMismatch, MachinePhase, MemberMismatch, MissingCandidate,
    PartitionOutOfRange, UnsubscribedTopic,
};
