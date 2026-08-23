//! Per-member hosted ownership between membership assignment and broker sessions.

use crate::config::ValidatedShareConsumerFetchConfig;
use kafka_client_core::{AssignmentGeneration, Deadline};

use super::{
    fetch_route::ShareFetchPartitionRouteFailureKind,
    fetch_routing::{ShareFetchRoutedAssignment, ShareFetchRoutingOwner},
};

/// One retained routing fault fenced by the membership assignment generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ShareFetchRoutingFault {
    generation: AssignmentGeneration,
    kind: ShareFetchPartitionRouteFailureKind,
}

/// Hosted pre-session routing and completed broker-plan ownership.
#[must_use = "share fetch entry state must remain with its registered member"]
pub(super) struct ShareFetchEntryState {
    config: ValidatedShareConsumerFetchConfig,
    routing: Option<ShareFetchRoutingOwner>,
    routed: Option<ShareFetchRoutedAssignment>,
    fault: Option<ShareFetchRoutingFault>,
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
            fault: None,
        }
    }

    pub(super) const fn config(&self) -> ValidatedShareConsumerFetchConfig {
        self.config
    }

    pub(super) fn unsettled(&self) -> usize {
        usize::from(self.routing.is_some()).saturating_add(usize::from(self.routed.is_some()))
    }

    pub(super) fn next_deadline(&self) -> Option<Deadline> {
        self.routing.as_ref().map(ShareFetchRoutingOwner::deadline)
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
}
