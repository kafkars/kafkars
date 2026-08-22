//! Infallible commit seam for one prepared KIP-848 member and assignment.

use std::sync::Arc;

use kafka_client_core::{ConsumerGroupMemberEpoch, LiveGroupAssignment, MemberId, MembershipCycle};

use super::session_catalog::{
    GroupSessionCatalog, GroupSessionCatalogError, MAX_KAFKA_GROUP_STRING_BYTES,
};

mod assignmentless;

/// Current modern member spelling paired with its engine assignment fence.
pub(super) struct ConsumerGroupSession {
    member_id: MemberId,
    member: Arc<str>,
    installed_cycle: MembershipCycle,
    member_epoch: Option<ConsumerGroupMemberEpoch>,
    assignment: Option<LiveGroupAssignment>,
}

impl ConsumerGroupSession {
    pub(super) const fn member_id(&self) -> MemberId {
        self.member_id
    }

    pub(super) const fn member(&self) -> &Arc<str> {
        &self.member
    }

    pub(super) const fn installed_cycle(&self) -> MembershipCycle {
        self.installed_cycle
    }

    pub(super) const fn member_epoch(&self) -> Option<ConsumerGroupMemberEpoch> {
        self.member_epoch
    }

    pub(super) const fn assignment(&self) -> Option<&LiveGroupAssignment> {
        self.assignment.as_ref()
    }
}

/// Broker spelling validated before deterministic success mutates core state.
pub(super) struct ConsumerGroupMemberCandidate {
    member_id: MemberId,
    member: Arc<str>,
    next_member_id: Option<MemberId>,
}

impl ConsumerGroupMemberCandidate {
    pub(super) const fn member_id(&self) -> MemberId {
        self.member_id
    }
}

impl GroupSessionCatalog {
    pub(super) fn prepare_consumer_group_member(
        &self,
        member: Arc<str>,
    ) -> Result<ConsumerGroupMemberCandidate, GroupSessionCatalogError> {
        validate_member(&member)?;
        if self.current.is_some() {
            return Err(GroupSessionCatalogError::SessionProtocolMismatch);
        }
        if let Some(current) = &self.consumer_current {
            if current.member.as_ref() != member.as_ref() {
                return Err(GroupSessionCatalogError::MemberMismatch);
            }
            return Ok(ConsumerGroupMemberCandidate {
                member_id: current.member_id,
                member,
                next_member_id: self.next_member_id,
            });
        }
        let member_id = self
            .next_member_id
            .ok_or(GroupSessionCatalogError::Allocation)?;
        let next_member_id = member_id
            .get()
            .checked_add(1)
            .and_then(MemberId::try_from_raw);
        Ok(ConsumerGroupMemberCandidate {
            member_id,
            member,
            next_member_id,
        })
    }

    pub(super) fn current_consumer_group_member_candidate(
        &self,
    ) -> Option<ConsumerGroupMemberCandidate> {
        let current = self.consumer_current.as_ref()?;
        Some(ConsumerGroupMemberCandidate {
            member_id: current.member_id,
            member: Arc::clone(&current.member),
            next_member_id: self.next_member_id,
        })
    }

    pub(super) fn commit_consumer_group_install(
        &mut self,
        candidate: ConsumerGroupMemberCandidate,
        installed_cycle: MembershipCycle,
        member_epoch: ConsumerGroupMemberEpoch,
        assignment: LiveGroupAssignment,
    ) {
        debug_assert!(self.current.is_none());
        debug_assert_eq!(assignment.group_id(), self.group_id());
        debug_assert_eq!(assignment.member_id(), candidate.member_id);
        self.next_member_id = candidate.next_member_id;
        self.required_join_member = None;
        self.consumer_current = Some(ConsumerGroupSession {
            member_id: candidate.member_id,
            member: candidate.member,
            installed_cycle,
            member_epoch: Some(member_epoch),
            assignment: Some(assignment),
        });
    }

    /// Advances broker membership while the exact prior assignment remains engine-owned.
    pub(super) fn commit_consumer_group_reconciliation_epoch(
        &mut self,
        candidate: &ConsumerGroupMemberCandidate,
        member_epoch: ConsumerGroupMemberEpoch,
    ) {
        let current = self
            .consumer_current
            .as_mut()
            .unwrap_or_else(|| unreachable!("reconciliation retains a modern member"));
        debug_assert_eq!(current.member_id, candidate.member_id);
        debug_assert_eq!(current.member.as_ref(), candidate.member.as_ref());
        current.member_epoch = Some(member_epoch);
    }

    /// Clears the retired assignment while retaining the current member and broker epoch.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "commit consumes the exact retired assignment so stale ownership cannot be reused"
    )]
    pub(super) fn commit_consumer_group_reconciliation_revoke(
        &mut self,
        assignment: LiveGroupAssignment,
    ) {
        let current = self
            .consumer_current
            .as_mut()
            .unwrap_or_else(|| unreachable!("reconciliation retains a modern member"));
        debug_assert_eq!(current.assignment.as_ref(), Some(&assignment));
        debug_assert!(current.member_epoch.is_some());
        current.assignment = None;
    }

    /// Installs the core-authorized target without replacing its retained modern member.
    pub(super) fn commit_consumer_group_reconciliation_install(
        &mut self,
        candidate: ConsumerGroupMemberCandidate,
        installed_cycle: MembershipCycle,
        member_epoch: ConsumerGroupMemberEpoch,
        assignment: LiveGroupAssignment,
    ) {
        let group_id = self.group_id();
        let current = self
            .consumer_current
            .as_mut()
            .unwrap_or_else(|| unreachable!("reconciliation retains a modern member"));
        debug_assert_eq!(current.member_id, candidate.member_id);
        debug_assert_eq!(current.member.as_ref(), candidate.member.as_ref());
        debug_assert!(
            current
                .member_epoch
                .is_some_and(|current| current <= member_epoch)
        );
        debug_assert_eq!(assignment.group_id(), group_id);
        debug_assert_eq!(assignment.member_id(), candidate.member_id);
        current.member = candidate.member;
        current.installed_cycle = installed_cycle;
        current.member_epoch = Some(member_epoch);
        current.assignment = Some(assignment);
        self.next_member_id = candidate.next_member_id;
        self.required_join_member = None;
    }

    #[allow(
        clippy::needless_pass_by_value,
        reason = "commit consumes the exact revoked assignment so stale ownership cannot be reused"
    )]
    pub(super) fn commit_consumer_group_revoke(&mut self, assignment: LiveGroupAssignment) {
        debug_assert!(
            self.consumer_current
                .as_ref()
                .and_then(ConsumerGroupSession::assignment)
                == Some(&assignment)
        );
        self.consumer_current = None;
    }

    /// Retains the process-lifetime Kafka member spelling while fencing its stale assignment.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "fencing consumes the exact stale assignment while preserving member identity"
    )]
    pub(super) fn commit_consumer_group_fenced_revoke(&mut self, assignment: LiveGroupAssignment) {
        let current = self
            .consumer_current
            .as_mut()
            .unwrap_or_else(|| unreachable!("fenced assignment has a modern member"));
        debug_assert_eq!(current.assignment.as_ref(), Some(&assignment));
        current.assignment = None;
        current.member_epoch = None;
    }

    pub(super) fn consumer_group_member_epoch(&self) -> Option<ConsumerGroupMemberEpoch> {
        self.consumer_current
            .as_ref()
            .and_then(ConsumerGroupSession::member_epoch)
    }
}

fn validate_member(member: &str) -> Result<(), GroupSessionCatalogError> {
    if member.is_empty() {
        return Err(GroupSessionCatalogError::EmptyMember);
    }
    if member.len() > MAX_KAFKA_GROUP_STRING_BYTES {
        return Err(GroupSessionCatalogError::MemberBytes {
            actual: member.len(),
            limit: MAX_KAFKA_GROUP_STRING_BYTES,
        });
    }
    Ok(())
}
