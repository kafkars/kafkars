//! Pure validation and topic-union preparation for classic transitions.

use crate::{GroupAssignmentPartition, MemberId, Moment, TopicId};

use super::model::MAX_CLASSIC_GROUP_TOPICS;
use super::{
    ClassicAssignmentPlan, ClassicGroupErrorKind, ClassicGroupMachine, ClassicGroupPhase,
    ClassicJoinMembers, JoinedMemberSlot, MembershipCycle,
};

pub(super) fn validate_active(
    machine: &ClassicGroupMachine,
    expected: ClassicGroupPhase,
    cycle: MembershipCycle,
    now: Moment,
) -> Result<crate::Deadline, ClassicGroupErrorKind> {
    if machine.phase != expected {
        return Err(ClassicGroupErrorKind::InvalidPhase);
    }
    if machine.active_cycle != Some(cycle) {
        return Err(ClassicGroupErrorKind::CycleMismatch);
    }
    match machine.deadline {
        Some(deadline) if deadline.is_elapsed_at(now) => {
            Err(ClassicGroupErrorKind::DeadlineElapsed)
        }
        Some(deadline) => Ok(deadline),
        None => Err(ClassicGroupErrorKind::InvariantViolation),
    }
}

pub(super) fn validate_inflight_cycle(
    machine: &ClassicGroupMachine,
    cycle: MembershipCycle,
) -> Result<(), ClassicGroupErrorKind> {
    if !matches!(
        machine.phase,
        ClassicGroupPhase::Joining
            | ClassicGroupPhase::AwaitingPartitionCounts
            | ClassicGroupPhase::Syncing
    ) {
        return Err(ClassicGroupErrorKind::InvalidPhase);
    }
    if machine.active_cycle != Some(cycle) {
        return Err(ClassicGroupErrorKind::CycleMismatch);
    }
    Ok(())
}

pub(super) fn validate_stage_cycle(
    machine: &ClassicGroupMachine,
    expected: ClassicGroupPhase,
    cycle: MembershipCycle,
) -> Result<(), ClassicGroupErrorKind> {
    if machine.phase != expected {
        return Err(ClassicGroupErrorKind::InvalidPhase);
    }
    if machine.active_cycle != Some(cycle) {
        return Err(ClassicGroupErrorKind::CycleMismatch);
    }
    Ok(())
}

pub(super) fn local_member_is_present(
    members: &ClassicJoinMembers,
    local_slot: JoinedMemberSlot,
    member_id: MemberId,
) -> bool {
    members
        .members()
        .iter()
        .any(|member| member.slot() == local_slot && member.member_id() == member_id)
}

pub(super) fn collect_group_topics(
    members: &ClassicJoinMembers,
) -> Result<Vec<TopicId>, ClassicGroupErrorKind> {
    let mut topics = Vec::new();
    for member in members.members() {
        for topic in member.subscription().topics() {
            if let Err(index) = topics.binary_search(topic) {
                if topics.len() == MAX_CLASSIC_GROUP_TOPICS {
                    return Err(ClassicGroupErrorKind::TooManyGroupTopics);
                }
                if topics.try_reserve_exact(1).is_err() {
                    return Err(ClassicGroupErrorKind::AllocationFailed);
                }
                topics.insert(index, *topic);
            }
        }
    }
    Ok(topics)
}

pub(super) fn copy_local_assignment(
    plan: &ClassicAssignmentPlan,
    local_slot: JoinedMemberSlot,
) -> Result<Vec<GroupAssignmentPartition>, ClassicGroupErrorKind> {
    let partitions = plan
        .entries()
        .iter()
        .find(|entry| entry.slot() == local_slot)
        .ok_or(ClassicGroupErrorKind::InvariantViolation)?
        .partitions();
    let mut copy = Vec::new();
    copy.try_reserve_exact(partitions.len())
        .map_err(|_| ClassicGroupErrorKind::AllocationFailed)?;
    copy.extend_from_slice(partitions);
    Ok(copy)
}

pub(super) const fn pair_error_kind(
    error: crate::LiveGroupAssignmentError,
) -> ClassicGroupErrorKind {
    match error {
        crate::LiveGroupAssignmentError::AllocationFailed => {
            ClassicGroupErrorKind::AllocationFailed
        }
        error => ClassicGroupErrorKind::InvalidLiveAssignment(error),
    }
}
