//! Same-cycle KIP-394 member-identity replacement for one Join.

use crate::{MemberId, Moment};

use super::{
    ClassicGroupEffect, ClassicGroupErrorKind, ClassicGroupMachine, ClassicGroupPhase,
    ClassicGroupTransition, MembershipCycle, transition_support::validate_active,
};

impl ClassicGroupMachine {
    pub(super) fn join_member_id_required(
        &mut self,
        cycle: MembershipCycle,
        now: Moment,
        assigned_member_id: Option<MemberId>,
    ) -> Result<ClassicGroupTransition, ClassicGroupErrorKind> {
        let deadline = validate_active(self, ClassicGroupPhase::Joining, cycle, now)?;
        let member_id = assigned_member_id.ok_or(ClassicGroupErrorKind::MissingAssignedMemberId)?;
        if self.pending_member_id.is_some() {
            return Err(ClassicGroupErrorKind::DuplicateAssignedMemberId);
        }
        if self.pending_generation.is_some()
            || self.pending_members.is_some()
            || self.pending_local_slot.is_some()
            || self.pending_expected_assignment.is_some()
            || self.pending_heartbeat_liveness.is_some()
        {
            return Err(ClassicGroupErrorKind::InvariantViolation);
        }
        let replacement = ClassicGroupEffect::Join {
            group_id: self.group_id,
            cycle,
            protocol: self.protocol(),
            member_id: Some(member_id),
            timing: self.timing(),
            deadline,
        };
        self.pending_member_id = Some(member_id);
        Ok(ClassicGroupTransition::one(replacement))
    }

    pub(super) fn validate_join_member_id(
        &self,
        member_id: MemberId,
    ) -> Result<(), ClassicGroupErrorKind> {
        if self
            .pending_member_id
            .is_some_and(|assigned| assigned != member_id)
        {
            Err(ClassicGroupErrorKind::AssignedMemberIdMismatch)
        } else {
            Ok(())
        }
    }
}
