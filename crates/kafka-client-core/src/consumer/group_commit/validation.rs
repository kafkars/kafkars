//! Borrowed preflight validation for assignment-fenced group checkpoints.

use super::{
    GroupAssignmentPartition, GroupCheckpoint, GroupOffsetCommitAdmissionErrorKind,
    LiveGroupAssignment,
};

/// Validates one checkpoint against the current live assignment without consuming it.
///
/// Execution layers may use this borrowed check before translating retained
/// topic identities. Admission repeats the same authoritative check after all
/// terminal and retained-capacity reservations have succeeded.
pub fn validate_group_offset_commit_checkpoint(
    live_assignment: Option<&LiveGroupAssignment>,
    checkpoint: &GroupCheckpoint,
) -> Result<(), GroupOffsetCommitAdmissionErrorKind> {
    let Some(assignment) = live_assignment else {
        return Err(GroupOffsetCommitAdmissionErrorKind::AssignmentLost);
    };
    if checkpoint.group_id() != assignment.group_id() {
        return Err(GroupOffsetCommitAdmissionErrorKind::GroupMismatch);
    }
    if checkpoint.member_id() != assignment.member_id() {
        return Err(GroupOffsetCommitAdmissionErrorKind::MemberMismatch);
    }
    if checkpoint.assignment_generation() != assignment.assignment_generation() {
        return Err(GroupOffsetCommitAdmissionErrorKind::GenerationMismatch);
    }
    if let Some(partition) = checkpoint
        .entries()
        .iter()
        .map(|entry| GroupAssignmentPartition::new(entry.topic_id(), entry.partition()))
        .find(|partition| assignment.partitions().binary_search(partition).is_err())
    {
        return Err(GroupOffsetCommitAdmissionErrorKind::UnassignedPartition {
            topic_id: partition.topic_id(),
            partition: partition.partition(),
        });
    }
    Ok(())
}
