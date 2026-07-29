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
            ClassicGroupInput::JoinMemberIdRequired {
                cycle,
                now,
                assigned_member_id,
            } => self.join_member_id_required(cycle, now, assigned_member_id),
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
            ClassicGroupInput::HeartbeatDue { attempt, now } => self.heartbeat_due(attempt, now),
            ClassicGroupInput::HeartbeatSucceeded {
                attempt,
                now,
                throttle_ticks,
            } => self.heartbeat_succeeded(attempt, now, throttle_ticks),
            ClassicGroupInput::HeartbeatRejected {
                attempt,
                now,
                error,
            } => self.heartbeat_rejected(attempt, now, error),
            ClassicGroupInput::HeartbeatCoordinatorLost { attempt, now } => {
                self.heartbeat_coordinator_lost(attempt, now)
            }
            ClassicGroupInput::HeartbeatFailed { attempt } => self.heartbeat_failed(attempt),
            ClassicGroupInput::HeartbeatDeadlineElapsed { attempt, now } => {
                self.heartbeat_deadline_elapsed(attempt, now)
            }
            ClassicGroupInput::JoinFailed { cycle } => self.join_failed(cycle),
            ClassicGroupInput::JoinRejected { cycle, now, error } => {
                self.join_rejected(cycle, now, error)
            }
            ClassicGroupInput::PartitionCountsFailed { cycle } => {
                self.partition_counts_failed(cycle)
            }
            ClassicGroupInput::SyncFailed { cycle } => self.sync_failed(cycle),
            ClassicGroupInput::SyncRejected { cycle, now, error } => {
                self.sync_rejected(cycle, now, error)
            }
            ClassicGroupInput::RejoinDue { schedule, now } => self.rejoin_due(schedule, now),
            ClassicGroupInput::AssignmentLost { cycle } => self.assignment_lost(cycle),
            ClassicGroupInput::DeadlineElapsed { cycle, now } => self.deadline_elapsed(cycle, now),
            ClassicGroupInput::Close => self.close(),
        };
        result.map_err(ClassicGroupApplyError::new)
    }
}
