//! Allocation-atomic cooperative assignment replacement and ordered ownership delta.

use core::cmp::Ordering;

use crate::{
    AssignmentGeneration, GroupAssignmentPartition, LiveGroupAssignment, LiveGroupAssignmentError,
    MemberId,
};

use super::{
    ClassicGeneration, ClassicGroupErrorKind, ClassicHeartbeatSchedule, MembershipCycle,
    transition_support::pair_error_kind,
};

/// Ordered ownership changes between two cooperative assignment generations.
#[derive(Debug, Eq, PartialEq)]
pub struct ClassicAssignmentDelta {
    retained: Vec<GroupAssignmentPartition>,
    removed: Vec<GroupAssignmentPartition>,
    added: Vec<GroupAssignmentPartition>,
}

impl ClassicAssignmentDelta {
    /// Borrows partitions owned before and after reconciliation.
    pub fn retained(&self) -> &[GroupAssignmentPartition] {
        &self.retained
    }

    /// Borrows partitions removed by reconciliation.
    pub fn removed(&self) -> &[GroupAssignmentPartition] {
        &self.removed
    }

    /// Borrows partitions added by reconciliation.
    pub fn added(&self) -> &[GroupAssignmentPartition] {
        &self.added
    }
}

/// Exact prior and replacement ownership emitted for engine-side reconciliation.
#[derive(Debug, Eq, PartialEq)]
pub struct ClassicAssignmentReconciliation {
    previous_cycle: MembershipCycle,
    previous_classic_generation: ClassicGeneration,
    previous_assignment: LiveGroupAssignment,
    replacement_cycle: MembershipCycle,
    replacement_classic_generation: ClassicGeneration,
    replacement_assignment: LiveGroupAssignment,
    heartbeat: ClassicHeartbeatSchedule,
    delta: ClassicAssignmentDelta,
    requires_followup: bool,
}

impl ClassicAssignmentReconciliation {
    /// Returns the membership cycle that owned the prior assignment.
    pub const fn previous_cycle(&self) -> MembershipCycle {
        self.previous_cycle
    }

    /// Returns the Kafka generation paired with the prior assignment.
    pub const fn previous_classic_generation(&self) -> ClassicGeneration {
        self.previous_classic_generation
    }

    /// Borrows the exact prior live assignment.
    pub const fn previous_assignment(&self) -> &LiveGroupAssignment {
        &self.previous_assignment
    }

    /// Returns the membership cycle that owns the replacement assignment.
    pub const fn replacement_cycle(&self) -> MembershipCycle {
        self.replacement_cycle
    }

    /// Returns the Kafka generation paired with the replacement assignment.
    pub const fn replacement_classic_generation(&self) -> ClassicGeneration {
        self.replacement_classic_generation
    }

    /// Borrows the exact replacement assignment.
    pub const fn replacement_assignment(&self) -> &LiveGroupAssignment {
        &self.replacement_assignment
    }

    /// Returns the first heartbeat schedule for the replacement assignment.
    pub const fn heartbeat(&self) -> ClassicHeartbeatSchedule {
        self.heartbeat
    }

    /// Borrows the deterministic ordered ownership delta.
    pub const fn delta(&self) -> &ClassicAssignmentDelta {
        &self.delta
    }

    /// Returns whether cooperative ownership needs another Join and Sync round.
    pub const fn requires_followup(&self) -> bool {
        self.requires_followup
    }

    /// Moves both assignment copies and their ordered delta to the interpreter.
    pub fn into_assignments(
        self,
    ) -> (
        LiveGroupAssignment,
        LiveGroupAssignment,
        ClassicAssignmentDelta,
    ) {
        (
            self.previous_assignment,
            self.replacement_assignment,
            self.delta,
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct PendingClassicReconciliation {
    cycle: MembershipCycle,
    assignment_generation: AssignmentGeneration,
    requires_followup: bool,
}

impl PendingClassicReconciliation {
    pub(super) const fn new(
        cycle: MembershipCycle,
        assignment_generation: AssignmentGeneration,
        requires_followup: bool,
    ) -> Self {
        Self {
            cycle,
            assignment_generation,
            requires_followup,
        }
    }

    pub(super) const fn cycle(self) -> MembershipCycle {
        self.cycle
    }

    pub(super) const fn assignment_generation(self) -> AssignmentGeneration {
        self.assignment_generation
    }

    pub(super) const fn requires_followup(self) -> bool {
        self.requires_followup
    }
}

pub(super) struct PreparedClassicReconciliation {
    pub(super) retained_assignment: LiveGroupAssignment,
    pub(super) effect: ClassicAssignmentReconciliation,
}

#[allow(clippy::too_many_arguments)]
pub(super) fn try_prepare_reconciliation(
    previous_cycle: MembershipCycle,
    previous_classic_generation: ClassicGeneration,
    previous_assignment: &LiveGroupAssignment,
    replacement_cycle: MembershipCycle,
    replacement_classic_generation: ClassicGeneration,
    replacement_member_id: MemberId,
    replacement_assignment_generation: AssignmentGeneration,
    replacement_partitions: Vec<GroupAssignmentPartition>,
    heartbeat: ClassicHeartbeatSchedule,
    leader_withheld_transfers: bool,
) -> Result<PreparedClassicReconciliation, ClassicGroupErrorKind> {
    let delta = try_delta(previous_assignment.partitions(), &replacement_partitions)?;
    let previous_effect_assignment = try_copy_assignment(previous_assignment)?;
    let (retained_assignment, replacement_effect_assignment) = LiveGroupAssignment::try_new_pair(
        previous_assignment.group_id(),
        replacement_member_id,
        replacement_assignment_generation,
        replacement_partitions,
    )
    .map_err(|(error, _)| pair_error_kind(error))?;
    let requires_followup = leader_withheld_transfers || !delta.removed.is_empty();
    Ok(PreparedClassicReconciliation {
        retained_assignment,
        effect: ClassicAssignmentReconciliation {
            previous_cycle,
            previous_classic_generation,
            previous_assignment: previous_effect_assignment,
            replacement_cycle,
            replacement_classic_generation,
            replacement_assignment: replacement_effect_assignment,
            heartbeat,
            delta,
            requires_followup,
        },
    })
}

fn try_copy_assignment(
    assignment: &LiveGroupAssignment,
) -> Result<LiveGroupAssignment, ClassicGroupErrorKind> {
    let mut partitions = Vec::new();
    partitions
        .try_reserve_exact(assignment.partitions().len())
        .map_err(|_| ClassicGroupErrorKind::AllocationFailed)?;
    partitions.extend_from_slice(assignment.partitions());
    LiveGroupAssignment::try_new(
        assignment.group_id(),
        assignment.member_id(),
        assignment.assignment_generation(),
        partitions,
    )
    .map_err(map_assignment_error)
}

fn map_assignment_error(error: LiveGroupAssignmentError) -> ClassicGroupErrorKind {
    match error {
        LiveGroupAssignmentError::AllocationFailed => ClassicGroupErrorKind::AllocationFailed,
        error => ClassicGroupErrorKind::InvalidLiveAssignment(error),
    }
}

fn try_delta(
    previous: &[GroupAssignmentPartition],
    replacement: &[GroupAssignmentPartition],
) -> Result<ClassicAssignmentDelta, ClassicGroupErrorKind> {
    let mut retained = try_partition_buffer(previous.len().min(replacement.len()))?;
    let mut removed = try_partition_buffer(previous.len())?;
    let mut added = try_partition_buffer(replacement.len())?;
    let (mut previous_index, mut replacement_index) = (0, 0);
    while previous_index < previous.len() || replacement_index < replacement.len() {
        match (
            previous.get(previous_index),
            replacement.get(replacement_index),
        ) {
            (Some(left), Some(right)) => match left.cmp(right) {
                Ordering::Equal => {
                    retained.push(*left);
                    previous_index += 1;
                    replacement_index += 1;
                }
                Ordering::Less => {
                    removed.push(*left);
                    previous_index += 1;
                }
                Ordering::Greater => {
                    added.push(*right);
                    replacement_index += 1;
                }
            },
            (Some(left), None) => {
                removed.push(*left);
                previous_index += 1;
            }
            (None, Some(right)) => {
                added.push(*right);
                replacement_index += 1;
            }
            (None, None) => break,
        }
    }
    Ok(ClassicAssignmentDelta {
        retained,
        removed,
        added,
    })
}

fn try_partition_buffer(
    capacity: usize,
) -> Result<Vec<GroupAssignmentPartition>, ClassicGroupErrorKind> {
    let mut partitions = Vec::new();
    partitions
        .try_reserve_exact(capacity)
        .map_err(|_| ClassicGroupErrorKind::AllocationFailed)?;
    Ok(partitions)
}
