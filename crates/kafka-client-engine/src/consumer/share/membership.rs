//! Closed engine owner for one share-member policy lifetime.

use kafka_client_core::{
    LiveGroupAssignment, ShareGroupHeartbeatErrorKind, ShareGroupHeartbeatFailure,
    ShareGroupHeartbeatMachine, ShareGroupHeartbeatPolicy, ShareGroupHeartbeatRetrySchedule,
};

use super::{catalog::ShareMembershipCatalogError, prepared::PreparedShareGroupHeartbeat};

use super::ShareMembershipCatalog;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer) enum ShareMembershipRetryGate {
    Open,
    CoordinatorLoad,
    Rediscovery {
        retry_due: bool,
        invalidation_complete: bool,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ShareMembershipError {
    Catalog(ShareMembershipCatalogError),
    Core(ShareGroupHeartbeatErrorKind),
    DeadlineMapping,
    EffectShape,
    Occupied,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer) enum ShareMembershipFailureTurn {
    Terminal,
    RetryScheduled(ShareGroupHeartbeatRetrySchedule),
    Rediscovery(ShareGroupHeartbeatRetrySchedule),
    Rejoin,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer) enum ShareMembershipRetryDueTurn {
    SubmissionReady,
    Terminal,
}

pub(in crate::consumer) struct ShareMembershipInterpreter {
    pub(super) machine: ShareGroupHeartbeatMachine,
    pub(super) catalog: ShareMembershipCatalog,
    pub(super) prepared: Option<PreparedShareGroupHeartbeat>,
    pub(super) activated_assignment: Option<LiveGroupAssignment>,
    pub(super) retry_gate: ShareMembershipRetryGate,
}

impl ShareMembershipInterpreter {
    pub(super) fn new(
        group_id: kafka_client_core::GroupId,
        member_id: kafka_client_core::MemberId,
        policy: ShareGroupHeartbeatPolicy,
        catalog: ShareMembershipCatalog,
    ) -> Self {
        Self {
            machine: ShareGroupHeartbeatMachine::new(group_id, member_id, policy),
            catalog,
            prepared: None,
            activated_assignment: None,
            retry_gate: ShareMembershipRetryGate::Open,
        }
    }

    pub(super) const fn machine(&self) -> &ShareGroupHeartbeatMachine {
        &self.machine
    }

    pub(super) const fn prepared(&self) -> Option<PreparedShareGroupHeartbeat> {
        self.prepared
    }

    pub(super) const fn retry_gate(&self) -> ShareMembershipRetryGate {
        self.retry_gate
    }

    pub(super) const fn activated_assignment(&self) -> Option<&LiveGroupAssignment> {
        self.activated_assignment.as_ref()
    }

    pub(super) fn is_ready_to_submit(&self) -> bool {
        self.prepared.is_some() && self.retry_gate == ShareMembershipRetryGate::Open
    }

    pub(super) fn next_deadline(&self) -> Option<kafka_client_core::Deadline> {
        [
            self.prepared.map(|prepared| prepared.deadline.core()),
            self.machine
                .retry_schedule()
                .map(kafka_client_core::ShareGroupHeartbeatRetrySchedule::not_before),
            self.machine
                .schedule()
                .map(kafka_client_core::ShareGroupHeartbeatSchedule::deadline),
        ]
        .into_iter()
        .flatten()
        .min()
    }

    pub(super) const fn startup_failure(&self) -> Option<ShareGroupHeartbeatFailure> {
        match self.machine.startup_fatal() {
            Some(fatal) => Some(fatal.failure()),
            None => None,
        }
    }
}
