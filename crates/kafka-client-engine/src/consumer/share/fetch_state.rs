//! Per-member hosted ownership between membership assignment and broker sessions.

use crate::config::ValidatedShareConsumerFetchConfig;
use kafka_client_core::{AssignmentGeneration, Deadline};

use super::{
    fetch_route::ShareFetchPartitionRouteFailureKind,
    fetch_routing::{ShareFetchRoutedAssignment, ShareFetchRoutingOwner},
    fetch_session_execution::ShareFetchExecutionError,
    fetch_session_set::{ShareFetchSessionSet, ShareFetchSessionSetOpenError},
};

/// One retained routing fault fenced by the membership assignment generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ShareFetchRoutingFault {
    generation: AssignmentGeneration,
    kind: ShareFetchPartitionRouteFailureKind,
}

/// One retained broker-session failure fenced by the membership assignment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ShareFetchSessionFault {
    generation: AssignmentGeneration,
    kind: ShareFetchSessionFaultKind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ShareFetchSessionFaultKind {
    Open(ShareFetchSessionSetOpenError),
    Execution(ShareFetchExecutionError),
    DeadlineMapping,
}

/// Hosted pre-session routing and completed broker-plan ownership.
#[must_use = "share fetch entry state must remain with its registered member"]
pub(super) struct ShareFetchEntryState {
    config: ValidatedShareConsumerFetchConfig,
    routing: Option<ShareFetchRoutingOwner>,
    routed: Option<ShareFetchRoutedAssignment>,
    sessions: Option<ShareFetchSessionSet>,
    fault: Option<ShareFetchRoutingFault>,
    session_fault: Option<ShareFetchSessionFault>,
}

impl ShareFetchRoutingFault {
    pub(super) const fn new(
        generation: AssignmentGeneration,
        kind: ShareFetchPartitionRouteFailureKind,
    ) -> Self {
        Self { generation, kind }
    }

    pub(super) const fn generation(self) -> AssignmentGeneration {
        self.generation
    }

    pub(super) const fn kind(self) -> ShareFetchPartitionRouteFailureKind {
        self.kind
    }
}

impl ShareFetchEntryState {
    pub(super) const fn new(config: ValidatedShareConsumerFetchConfig) -> Self {
        Self {
            config,
            routing: None,
            routed: None,
            sessions: None,
            fault: None,
            session_fault: None,
        }
    }

    pub(super) const fn config(&self) -> ValidatedShareConsumerFetchConfig {
        self.config
    }

    pub(super) fn unsettled(&self) -> usize {
        usize::from(self.routing.is_some())
            .saturating_add(usize::from(self.routed.is_some()))
            .saturating_add(self.sessions.as_ref().map_or(0, ShareFetchSessionSet::len))
    }

    pub(super) fn next_deadline(&self) -> Option<Deadline> {
        [
            self.routing.as_ref().map(ShareFetchRoutingOwner::deadline),
            self.sessions
                .as_ref()
                .and_then(ShareFetchSessionSet::next_deadline),
        ]
        .into_iter()
        .flatten()
        .min()
    }

    pub(super) fn routing(&self) -> Option<&ShareFetchRoutingOwner> {
        self.routing.as_ref()
    }

    pub(super) fn routing_mut(&mut self) -> Option<&mut ShareFetchRoutingOwner> {
        self.routing.as_mut()
    }

    pub(super) fn install_routing(
        &mut self,
        routing: ShareFetchRoutingOwner,
    ) -> Option<ShareFetchRoutingOwner> {
        if self.routing.is_some() {
            return Some(routing);
        }
        self.routing = Some(routing);
        None
    }

    pub(super) fn take_routing(&mut self) -> Option<ShareFetchRoutingOwner> {
        self.routing.take()
    }

    pub(super) fn routed(&self) -> Option<&ShareFetchRoutedAssignment> {
        self.routed.as_ref()
    }

    pub(super) fn install_routed(
        &mut self,
        routed: ShareFetchRoutedAssignment,
    ) -> Option<ShareFetchRoutedAssignment> {
        if self.routed.is_some() {
            return Some(routed);
        }
        self.routed = Some(routed);
        None
    }

    pub(super) fn take_routed(&mut self) -> Option<ShareFetchRoutedAssignment> {
        self.routed.take()
    }

    pub(super) fn sessions(&self) -> Option<&ShareFetchSessionSet> {
        self.sessions.as_ref()
    }

    pub(super) fn sessions_mut(&mut self) -> Option<&mut ShareFetchSessionSet> {
        self.sessions.as_mut()
    }

    pub(super) fn install_sessions(
        &mut self,
        sessions: ShareFetchSessionSet,
    ) -> Option<ShareFetchSessionSet> {
        if self.sessions.is_some() {
            return Some(sessions);
        }
        self.sessions = Some(sessions);
        None
    }

    pub(super) fn take_sessions(&mut self) -> Option<ShareFetchSessionSet> {
        self.sessions.take()
    }

    pub(super) const fn fault(&self) -> Option<ShareFetchRoutingFault> {
        self.fault
    }

    pub(super) fn retain_fault(&mut self, fault: ShareFetchRoutingFault) -> bool {
        if self.fault.is_some() {
            return false;
        }
        self.fault = Some(fault);
        true
    }

    pub(super) fn clear_fault(&mut self) {
        self.fault = None;
    }

    pub(super) const fn session_fault(&self) -> Option<ShareFetchSessionFault> {
        self.session_fault
    }

    pub(super) fn retain_session_fault(&mut self, fault: ShareFetchSessionFault) -> bool {
        if self.session_fault.is_some() {
            return false;
        }
        self.session_fault = Some(fault);
        true
    }

    pub(super) fn clear_session_fault(&mut self) {
        self.session_fault = None;
    }

    pub(super) const fn blocks_close(&self) -> bool {
        self.routing.is_some() || self.routed.is_some() || self.sessions.is_some()
    }
}

impl ShareFetchSessionFault {
    pub(super) const fn new(
        generation: AssignmentGeneration,
        kind: ShareFetchSessionFaultKind,
    ) -> Self {
        Self { generation, kind }
    }

    pub(super) const fn generation(self) -> AssignmentGeneration {
        self.generation
    }

    pub(super) const fn kind(self) -> ShareFetchSessionFaultKind {
        self.kind
    }
}
