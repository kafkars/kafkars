//! Assignmentless modern-member installation, fencing, and terminal close commits.

use kafka_client_core::{ConsumerGroupMemberEpoch, MembershipCycle};

use super::{ConsumerGroupMemberCandidate, ConsumerGroupSession, GroupSessionCatalog};

impl GroupSessionCatalog {
    pub(in crate::consumer::group) fn commit_consumer_group_awaiting_assignment(
        &mut self,
        candidate: ConsumerGroupMemberCandidate,
        installed_cycle: MembershipCycle,
        member_epoch: ConsumerGroupMemberEpoch,
    ) {
        debug_assert!(self.current.is_none());
        match self.consumer_current.as_mut() {
            Some(current) => {
                debug_assert_eq!(current.member_id, candidate.member_id);
                debug_assert_eq!(current.member.as_ref(), candidate.member.as_ref());
                debug_assert!(current.assignment.is_none());
                current.member = candidate.member;
                current.installed_cycle = installed_cycle;
                current.member_epoch = Some(member_epoch);
            }
            None => {
                self.consumer_current = Some(ConsumerGroupSession {
                    member_id: candidate.member_id,
                    member: candidate.member,
                    installed_cycle,
                    member_epoch: Some(member_epoch),
                    assignment: None,
                });
            }
        }
        self.next_member_id = candidate.next_member_id;
        self.required_join_member = None;
    }

    pub(in crate::consumer::group) fn commit_consumer_group_fenced_without_assignment(&mut self) {
        let current = self
            .consumer_current
            .as_mut()
            .unwrap_or_else(|| unreachable!("fenced member remains retained"));
        debug_assert!(current.assignment.is_none());
        current.member_epoch = None;
    }

    pub(in crate::consumer::group) fn commit_consumer_group_close_without_assignment(&mut self) {
        debug_assert!(
            self.consumer_current
                .as_ref()
                .is_some_and(|current| current.assignment.is_none())
        );
        self.consumer_current = None;
    }

    pub(in crate::consumer::group) fn commit_assignmentless_consumer_group_close(&mut self) {
        if self
            .consumer_current
            .as_ref()
            .is_some_and(|current| current.assignment.is_none())
        {
            self.commit_consumer_group_close_without_assignment();
        }
    }
}
