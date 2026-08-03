//! Bounded deterministic sticky planning with cooperative ownership handoff.

use crate::{GroupAssignmentPartition, PartitionIndex};

use super::{
    ClassicAssignmentError, ClassicJoinMember, ClassicJoinMembers, ClassicMemberAssignment,
    JoinedMemberSlot, TopicPartitionCount, assignment::MAX_CLASSIC_MEMBER_PARTITIONS,
};

struct TargetMember {
    slot: JoinedMemberSlot,
    partitions: Vec<GroupAssignmentPartition>,
}

pub(super) fn try_plan(
    members: &ClassicJoinMembers,
    counts: &[TopicPartitionCount],
) -> Result<(Vec<ClassicMemberAssignment>, usize), ClassicAssignmentError> {
    validate_unique_ownership(members)?;
    let mut targets = empty_targets(members)?;
    retain_valid_ownership(members, counts, &mut targets)?;
    assign_unowned(members, counts, &mut targets)?;
    rebalance(members, &mut targets)?;
    validate_member_bounds(members, &targets)?;
    cooperative_projection(members, targets)
}

fn empty_targets(
    members: &ClassicJoinMembers,
) -> Result<Vec<TargetMember>, ClassicAssignmentError> {
    let mut targets = Vec::new();
    targets
        .try_reserve_exact(members.members().len())
        .map_err(|_| ClassicAssignmentError::AllocationFailed)?;
    targets.extend(members.members().iter().map(|member| TargetMember {
        slot: member.slot(),
        partitions: Vec::new(),
    }));
    Ok(targets)
}

fn validate_unique_ownership(members: &ClassicJoinMembers) -> Result<(), ClassicAssignmentError> {
    for member in members.members() {
        for partition in member.subscription().owned_partitions() {
            authoritative_owner(members, *partition)?;
        }
    }
    Ok(())
}

fn retain_valid_ownership(
    members: &ClassicJoinMembers,
    counts: &[TopicPartitionCount],
    targets: &mut [TargetMember],
) -> Result<(), ClassicAssignmentError> {
    for (member_index, (member, target)) in members.members().iter().zip(targets).enumerate() {
        target
            .partitions
            .try_reserve_exact(member.subscription().owned_partitions().len())
            .map_err(|_| ClassicAssignmentError::AllocationFailed)?;
        for partition in member.subscription().owned_partitions() {
            if partition_exists(*partition, counts)
                && subscribed(member, *partition)
                && authoritative_owner(members, *partition)? == Some(member_index)
            {
                target.partitions.push(*partition);
            }
        }
    }
    Ok(())
}

fn assign_unowned(
    members: &ClassicJoinMembers,
    counts: &[TopicPartitionCount],
    targets: &mut [TargetMember],
) -> Result<(), ClassicAssignmentError> {
    for count in counts {
        for raw_partition in 0..count.count() {
            let partition = GroupAssignmentPartition::new(
                count.topic_id(),
                PartitionIndex::from_raw(raw_partition),
            );
            if targets
                .iter()
                .any(|target| target.partitions.binary_search(&partition).is_ok())
            {
                continue;
            }
            let recipient = members
                .members()
                .iter()
                .enumerate()
                .filter(|(_, member)| subscribed(member, partition))
                .min_by_key(|(index, _)| (targets[*index].partitions.len(), *index))
                .map(|(index, _)| index)
                .ok_or(ClassicAssignmentError::ArithmeticOverflow)?;
            insert_partition(&mut targets[recipient].partitions, partition)?;
        }
    }
    Ok(())
}

fn rebalance(
    members: &ClassicJoinMembers,
    targets: &mut [TargetMember],
) -> Result<(), ClassicAssignmentError> {
    loop {
        let mut selected = None;
        for recipient in 0..targets.len() {
            for donor in 0..targets.len() {
                if targets[donor].partitions.len() <= targets[recipient].partitions.len() + 1 {
                    continue;
                }
                let Some(partition_index) = targets[donor]
                    .partitions
                    .iter()
                    .rposition(|partition| subscribed(&members.members()[recipient], *partition))
                else {
                    continue;
                };
                let key = (
                    targets[recipient].partitions.len(),
                    usize::MAX - targets[donor].partitions.len(),
                    recipient,
                    donor,
                );
                if selected
                    .as_ref()
                    .is_none_or(|(selected_key, _, _, _)| key < *selected_key)
                {
                    selected = Some((key, donor, recipient, partition_index));
                }
            }
        }
        let Some((_key, donor, recipient, partition_index)) = selected else {
            break;
        };
        targets[recipient]
            .partitions
            .try_reserve(1)
            .map_err(|_| ClassicAssignmentError::AllocationFailed)?;
        let partition = targets[donor].partitions.remove(partition_index);
        let insert_at = targets[recipient]
            .partitions
            .binary_search(&partition)
            .unwrap_or_else(|index| index);
        targets[recipient].partitions.insert(insert_at, partition);
    }
    Ok(())
}

fn validate_member_bounds(
    members: &ClassicJoinMembers,
    targets: &[TargetMember],
) -> Result<(), ClassicAssignmentError> {
    for (member, target) in members.members().iter().zip(targets) {
        if target.partitions.len() > MAX_CLASSIC_MEMBER_PARTITIONS {
            return Err(ClassicAssignmentError::MemberPartitionLimit {
                member_id: member.member_id(),
                actual: target.partitions.len(),
            });
        }
    }
    Ok(())
}

fn cooperative_projection(
    members: &ClassicJoinMembers,
    targets: Vec<TargetMember>,
) -> Result<(Vec<ClassicMemberAssignment>, usize), ClassicAssignmentError> {
    let mut assignments = Vec::new();
    assignments
        .try_reserve_exact(targets.len())
        .map_err(|_| ClassicAssignmentError::AllocationFailed)?;
    let mut withheld_transfers = 0_usize;
    for (member_index, target) in targets.into_iter().enumerate() {
        let mut partitions = Vec::new();
        partitions
            .try_reserve_exact(target.partitions.len())
            .map_err(|_| ClassicAssignmentError::AllocationFailed)?;
        for partition in target.partitions {
            match current_owner(members, partition)? {
                Some(owner) if owner != member_index => withheld_transfers += 1,
                _ => partitions.push(partition),
            }
        }
        assignments.push(ClassicMemberAssignment {
            slot: target.slot,
            partitions,
        });
    }
    Ok((assignments, withheld_transfers))
}

fn current_owner(
    members: &ClassicJoinMembers,
    partition: GroupAssignmentPartition,
) -> Result<Option<usize>, ClassicAssignmentError> {
    authoritative_owner(members, partition)
}

fn authoritative_owner(
    members: &ClassicJoinMembers,
    partition: GroupAssignmentPartition,
) -> Result<Option<usize>, ClassicAssignmentError> {
    let mut selected: Option<(usize, Option<super::ClassicGeneration>)> = None;
    for (index, member) in members.members().iter().enumerate() {
        if member
            .subscription()
            .owned_partitions()
            .binary_search(&partition)
            .is_err()
        {
            continue;
        }
        let generation = member.subscription().generation();
        match selected {
            None => selected = Some((index, generation)),
            Some((_, selected_generation)) if generation > selected_generation => {
                selected = Some((index, generation));
            }
            Some((_, selected_generation)) if generation == selected_generation => {
                return Err(ClassicAssignmentError::ConflictingOwnedPartition(partition));
            }
            Some(_) => {}
        }
    }
    Ok(selected.map(|(index, _)| index))
}

fn partition_exists(partition: GroupAssignmentPartition, counts: &[TopicPartitionCount]) -> bool {
    counts.iter().any(|count| {
        count.topic_id() == partition.topic_id() && partition.partition().get() < count.count()
    })
}

fn subscribed(member: &ClassicJoinMember, partition: GroupAssignmentPartition) -> bool {
    member
        .subscription()
        .topics()
        .binary_search(&partition.topic_id())
        .is_ok()
}

fn insert_partition(
    partitions: &mut Vec<GroupAssignmentPartition>,
    partition: GroupAssignmentPartition,
) -> Result<(), ClassicAssignmentError> {
    let index = partitions
        .binary_search(&partition)
        .unwrap_or_else(|index| index);
    partitions
        .try_reserve(1)
        .map_err(|_| ClassicAssignmentError::AllocationFailed)?;
    partitions.insert(index, partition);
    Ok(())
}
