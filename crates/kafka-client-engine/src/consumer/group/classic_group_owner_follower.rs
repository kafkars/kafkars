//! Atomic follower candidate staging and exact empty-Sync preparation.

use kafka_client_core::{ClassicGeneration, ClassicGroupEffect, ClassicGroupInput, Moment};

use crate::{clock::OperationDeadline, protocol::consumer::classic_follower_sync_group_request};

use super::{
    classic_group_candidate::ClassicGroupCycleCandidate,
    classic_group_owner::{ClassicGroupCandidateOwnershipError, ClassicGroupOwner},
    classic_group_sync::{ClassicGroupSyncIdentity, PreparedClassicGroupSync},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ClassicGroupFollowerJoinError {
    Candidate(ClassicGroupCandidateOwnershipError),
    Core(kafka_client_core::ClassicGroupErrorKind),
    UnexpectedSyncEffect,
    SyncRequest,
}

impl ClassicGroupOwner {
    pub(super) fn apply_follower_join(
        &mut self,
        group: &str,
        candidate: ClassicGroupCycleCandidate,
        generation: ClassicGeneration,
        now: Moment,
        deadline: OperationDeadline,
    ) -> Result<PreparedClassicGroupSync, ClassicGroupFollowerJoinError> {
        self.validate_candidate(&candidate)
            .map_err(ClassicGroupFollowerJoinError::Candidate)?;
        let cycle = candidate.cycle();
        let member_id = candidate.local_member_id();
        let member = candidate.local_member().clone();
        self.pending = Some(candidate);
        let transition = self
            .apply(ClassicGroupInput::JoinFollower {
                cycle,
                now,
                member_id,
                generation,
            })
            .map_err(|error| ClassicGroupFollowerJoinError::Core(error.kind()))?;
        let mut effects = transition.into_effects();
        let exact = match effects.next() {
            Some(ClassicGroupEffect::Sync {
                group_id,
                cycle: effect_cycle,
                member_id: effect_member,
                generation: effect_generation,
                plan,
                deadline: effect_deadline,
            }) => {
                group_id == self.machine().group_id()
                    && effect_cycle == cycle
                    && effect_member == member_id
                    && effect_generation == generation
                    && plan.entries().is_empty()
                    && effect_deadline == deadline.core()
                    && effects.next().is_none()
            }
            _ => false,
        };
        if !exact {
            return Err(ClassicGroupFollowerJoinError::UnexpectedSyncEffect);
        }
        let request = classic_follower_sync_group_request(group, &member, generation)
            .map_err(|_error| ClassicGroupFollowerJoinError::SyncRequest)?;
        Ok(PreparedClassicGroupSync::new(
            ClassicGroupSyncIdentity::new(
                self.machine().group_id(),
                cycle,
                member_id,
                generation,
                deadline,
            ),
            request,
        ))
    }

    pub(super) fn validate_candidate(
        &self,
        candidate: &ClassicGroupCycleCandidate,
    ) -> Result<(), ClassicGroupCandidateOwnershipError> {
        if self.machine().phase() != kafka_client_core::ClassicGroupPhase::Joining {
            return Err(ClassicGroupCandidateOwnershipError::Phase);
        }
        if self.machine().active_cycle() != Some(candidate.cycle()) {
            return Err(ClassicGroupCandidateOwnershipError::Cycle);
        }
        if self.pending.is_some() {
            return Err(ClassicGroupCandidateOwnershipError::Occupied);
        }
        Ok(())
    }
}
