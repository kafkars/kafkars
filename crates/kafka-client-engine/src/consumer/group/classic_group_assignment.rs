//! Exact preparation and infallible commit of core Install and Revoke effects.

use kafka_client_core::{
    ClassicGeneration, ClassicGroupPhase, LiveGroupAssignment, PartitionIndex,
};

use super::{classic_group_owner::ClassicGroupOwner, session_catalog::GroupSessionCatalog};

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
            ClassicGroupPhase::Lost | ClassicGroupPhase::Closed
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
