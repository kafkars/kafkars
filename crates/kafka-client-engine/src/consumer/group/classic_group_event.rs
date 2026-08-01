//! Bounded observation lifecycle derived from confirmed assignment transitions.

mod naming;

use std::collections::VecDeque;

use kafka_client_core::{ClassicGroupPhase, LiveGroupAssignment};

use crate::consumer::{GroupConsumerAssignment, GroupConsumerEvent};

use self::naming::named_assignment;
use super::session_catalog::GroupSessionCatalog;

/// One staged Sync observation plus bounded prior-loss/current-assignment state.
pub(super) struct ClassicGroupEventStore {
    staged_assignment: Option<GroupConsumerAssignment>,
    confirmed_assignment_epoch: Option<u64>,
    revoking_assignment_epoch: Option<u64>,
    ready: VecDeque<GroupConsumerEvent>,
}

impl ClassicGroupEventStore {
    pub(super) const fn new() -> Self {
        Self {
            staged_assignment: None,
            confirmed_assignment_epoch: None,
            revoking_assignment_epoch: None,
            ready: VecDeque::new(),
        }
    }

    pub(super) fn stage_assignment(&mut self, assignment: GroupConsumerAssignment) {
        self.staged_assignment = Some(assignment);
    }

    /// Publishes a staged assignment only after driver settlement confirms Sync.
    pub(super) fn confirm_sync(&mut self) {
        let Some(assignment) = self.staged_assignment.take() else {
            return;
        };
        self.revoking_assignment_epoch = None;
        self.confirmed_assignment_epoch = Some(assignment.assignment_epoch());
        self.publish(GroupConsumerEvent::PartitionsAssigned(assignment));
    }

    /// Publishes one bounded graceful-release event before physical retirement.
    pub(super) fn stage_graceful_revocation(
        &mut self,
        assignment: Option<GroupConsumerAssignment>,
        epoch: u64,
    ) {
        let Some(assignment) = assignment else {
            return;
        };
        debug_assert_eq!(assignment.assignment_epoch(), epoch);
        debug_assert_eq!(self.confirmed_assignment_epoch, Some(epoch));
        self.revoking_assignment_epoch = Some(epoch);
        self.publish(GroupConsumerEvent::PartitionsRevoked(assignment));
    }

    /// Lets KIP-848 publish loss only when graceful release ends unacknowledged.
    pub(super) fn lose_graceful_revocation(&mut self, epoch: u64) {
        if self.revoking_assignment_epoch == Some(epoch) {
            self.revoking_assignment_epoch = None;
        }
    }

    /// Retires the exact observation fence beside the authoritative assignment.
    pub(super) fn observe_retirement(
        &mut self,
        assignment: Option<GroupConsumerAssignment>,
        epoch: u64,
        phase: ClassicGroupPhase,
    ) {
        if phase == ClassicGroupPhase::Closed {
            self.staged_assignment = None;
            self.confirmed_assignment_epoch = None;
            self.revoking_assignment_epoch = None;
            self.ready.clear();
            return;
        }
        if self
            .staged_assignment
            .as_ref()
            .is_some_and(|staged| staged.assignment_epoch() == epoch)
        {
            self.staged_assignment = None;
        }
        if self.confirmed_assignment_epoch != Some(epoch) {
            return;
        }
        self.confirmed_assignment_epoch = None;
        if self.revoking_assignment_epoch == Some(epoch) {
            self.revoking_assignment_epoch = None;
            return;
        }
        match phase {
            ClassicGroupPhase::WaitingToRejoin
            | ClassicGroupPhase::Lost
            | ClassicGroupPhase::Fatal => {
                let assignment = assignment.unwrap_or_else(|| {
                    unreachable!("confirmed retirement retains its named assignment")
                });
                self.publish(GroupConsumerEvent::PartitionsLost(assignment));
            }
            ClassicGroupPhase::Closed => unreachable!("close returns before loss publication"),
            _ => unreachable!("core permits retirement only from a terminal or rejoin phase"),
        }
    }

    pub(super) fn take(&mut self) -> Option<GroupConsumerEvent> {
        self.ready.pop_front()
    }

    pub(super) const fn is_confirmed(&self, assignment_epoch: u64) -> bool {
        matches!(
            self.confirmed_assignment_epoch,
            Some(confirmed) if confirmed == assignment_epoch
        )
    }

    /// Retains at most the previous terminal event and current assignment state.
    fn publish(&mut self, event: GroupConsumerEvent) {
        let replace_back = matches!(
            (self.ready.back(), &event),
            (
                Some(GroupConsumerEvent::PartitionsAssigned(assigned)),
                GroupConsumerEvent::PartitionsLost(lost),
            ) if assigned.assignment_epoch() == lost.assignment_epoch()
        ) || matches!(
            (self.ready.back(), &event),
            (
                Some(GroupConsumerEvent::PartitionsAssigned(assigned)),
                GroupConsumerEvent::PartitionsRevoked(revoked),
            ) if assigned.assignment_epoch() == revoked.assignment_epoch()
        ) || matches!(
            (self.ready.back(), &event),
            (
                Some(GroupConsumerEvent::PartitionsRevoked(revoked)),
                GroupConsumerEvent::PartitionsLost(lost),
            ) if revoked.assignment_epoch() == lost.assignment_epoch()
        );
        if replace_back {
            let _superseded = self.ready.pop_back();
        }
        let coalesce_back = matches!(
            (self.ready.back(), &event),
            (
                Some(GroupConsumerEvent::PartitionsLost(older)),
                GroupConsumerEvent::PartitionsLost(newer),
            ) if older.assignment_epoch() < newer.assignment_epoch()
        ) || matches!(
            (self.ready.back(), &event),
            (
                Some(GroupConsumerEvent::PartitionsRevoked(older)),
                GroupConsumerEvent::PartitionsRevoked(newer),
            ) if older.assignment_epoch() < newer.assignment_epoch()
        );
        if coalesce_back {
            let _subsumed = self.ready.pop_back();
        }
        assert!(
            self.ready.len() < 2,
            "classic-group event lifecycle has at most loss plus assignment"
        );
        self.ready.push_back(event);
    }
}

impl GroupSessionCatalog {
    pub(super) fn stage_installed_assignment_event(&mut self) {
        let assignment = self
            .live_assignment()
            .unwrap_or_else(|| unreachable!("committed Sync retains its live assignment"));
        let named = named_assignment(self, assignment);
        self.events.stage_assignment(named);
    }

    pub(super) fn confirm_sync_event(&mut self) {
        self.events.confirm_sync();
    }

    pub(super) fn prepare_assignment_retirement_event(
        &self,
        assignment: &LiveGroupAssignment,
    ) -> Option<GroupConsumerAssignment> {
        let epoch = assignment.assignment_generation().get();
        (self.events.confirmed_assignment_epoch == Some(epoch)
            && self.events.revoking_assignment_epoch != Some(epoch))
        .then(|| named_assignment(self, assignment))
    }

    pub(super) fn prepare_graceful_revocation_event(
        &self,
        assignment: &LiveGroupAssignment,
    ) -> Option<GroupConsumerAssignment> {
        let epoch = assignment.assignment_generation().get();
        (self.events.confirmed_assignment_epoch == Some(epoch)
            && self.events.revoking_assignment_epoch.is_none())
        .then(|| named_assignment(self, assignment))
    }

    pub(super) fn commit_graceful_revocation_event(
        &mut self,
        named: Option<GroupConsumerAssignment>,
        epoch: u64,
    ) {
        self.events.stage_graceful_revocation(named, epoch);
    }

    pub(super) fn lose_consumer_group_graceful_revocation(&mut self, epoch: u64) {
        self.events.lose_graceful_revocation(epoch);
    }

    pub(super) fn commit_assignment_retirement_event(
        &mut self,
        named: Option<GroupConsumerAssignment>,
        epoch: u64,
        phase: ClassicGroupPhase,
    ) {
        self.events.observe_retirement(named, epoch, phase);
    }

    pub(super) fn take_event(&mut self) -> Option<GroupConsumerEvent> {
        self.events.take()
    }
}
