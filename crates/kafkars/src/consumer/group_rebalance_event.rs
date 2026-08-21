//! Linear public ownership of classic-group assignment transitions.

use crate::{
    KafkaError,
    bridge::consumer_facade::group_consumer_rebalance_event::GroupConsumerRevocationCompletion,
};

use super::{ConsumerAssignment, ConsumerAssignmentPartition};

/// One application-visible classic-group assignment transition.
///
/// Observation retains the previous terminal transition followed by the
/// current assignment. An unobserved assignment may be superseded by an exact
/// revoking or lost transition without manufacturing a callback.
#[derive(Debug)]
pub enum ConsumerEvent {
    /// Sync installed this assignment and driver settlement confirmed it.
    PartitionsAssigned(ConsumerAssignment),
    /// This assignment entered bounded graceful release.
    PartitionsRevoking(ConsumerRevocation),
    /// This assignment was retired; its and every older checkpoint is stale.
    PartitionsLost(ConsumerAssignment),
}

/// Event-owned, assignment-fenced graceful-revocation lease.
///
/// Dropping this value does not acknowledge the lease. The engine continues
/// membership work until explicit completion or the original absolute
/// revocation deadline retires the assignment.
#[derive(Debug)]
pub struct ConsumerRevocation {
    assignment: ConsumerAssignment,
    completion: GroupConsumerRevocationCompletion,
}

impl ConsumerRevocation {
    pub(crate) const fn from_parts(
        assignment: ConsumerAssignment,
        completion: GroupConsumerRevocationCompletion,
    ) -> Self {
        Self {
            assignment,
            completion,
        }
    }

    /// Borrows the exact assignment entering graceful release.
    pub const fn assignment(&self) -> &ConsumerAssignment {
        &self.assignment
    }

    /// Returns the nonreused assignment fence owned by this lease.
    pub const fn assignment_epoch(&self) -> u64 {
        self.assignment.assignment_epoch()
    }

    /// Borrows the ordered unique topic-partitions entering release.
    pub fn partitions(&self) -> &[ConsumerAssignmentPartition] {
        self.assignment.partitions()
    }

    /// Completes this exact lease under its existing absolute deadline.
    ///
    /// Contention leaves the lease available for another attempt. Repeated
    /// calls after success are inert successes.
    pub fn complete(&mut self) -> Result<(), KafkaError> {
        self.completion.complete()
    }
}
