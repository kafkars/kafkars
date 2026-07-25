//! Exhaustive dispatch from normalized inputs to their transition owners.

use super::{
    ClassicGroupApplyError, ClassicGroupInput, ClassicGroupMachine, ClassicGroupTransition,
};

impl ClassicGroupMachine {
    /// Applies one normalized fact without I/O, ambient time, callbacks, or retry.
    pub fn apply(
        &mut self,
        input: ClassicGroupInput,
    ) -> Result<ClassicGroupTransition, ClassicGroupApplyError> {
        let result = match input {
            ClassicGroupInput::Begin { now, deadline } => self.begin(now, deadline),
            ClassicGroupInput::JoinFollower {
                cycle,
                now,
                member_id,
                generation,
            } => self.join_follower(cycle, now, member_id, generation),
            ClassicGroupInput::JoinLeader {
                cycle,
                now,
                member_id,
                local_slot,
                generation,
                members,
            } => self.join_leader(cycle, now, member_id, local_slot, generation, members),
            ClassicGroupInput::PartitionCounts { cycle, now, counts } => {
                self.partition_counts(cycle, now, &counts)
            }
            ClassicGroupInput::SyncSucceeded {
                cycle,
                now,
                partitions,
            } => self.sync_succeeded(cycle, now, partitions),
            ClassicGroupInput::JoinFailed { cycle } => self.join_failed(cycle),
            ClassicGroupInput::PartitionCountsFailed { cycle } => {
                self.partition_counts_failed(cycle)
            }
            ClassicGroupInput::SyncFailed { cycle } => self.sync_failed(cycle),
            ClassicGroupInput::AssignmentLost { cycle } => self.assignment_lost(cycle),
            ClassicGroupInput::DeadlineElapsed { cycle, now } => self.deadline_elapsed(cycle, now),
            ClassicGroupInput::Close => self.close(),
        };
        result.map_err(ClassicGroupApplyError::new)
    }
}
