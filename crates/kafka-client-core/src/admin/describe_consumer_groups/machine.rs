//! Single-owner lifecycle vocabulary for consumer-group description.

use core::fmt;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    AdminConsumerGroupDescriptionOutcome, AdminDescribeConsumerGroupsPlan,
    AdminDescribeConsumerGroupsTerminal,
};

/// Current ownership stage for one consumer-group description operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminDescribeConsumerGroupsState {
    /// Accepted but not started.
    Ready,
    /// One exact group awaits driver admission.
    AwaitingDriver,
    /// The driver owns one exact coordinator call.
    Submitted,
    /// Core assigned the sole terminal decision.
    Completed,
}

/// Concrete Kafka protocol attempted for the current group.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminDescribeConsumerGroupsCallKind {
    /// KIP-848 `ConsumerGroupDescribe`, attempted first for every group.
    Consumer,
    /// Classic `DescribeGroups`, attempted once after an explicit fallback fact.
    ClassicFallback,
}

/// One normalized fact applied to deterministic policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdminDescribeConsumerGroupsInput {
    /// Starts execution at the supplied monotonic observation.
    Start {
        /// Current monotonic observation.
        now: Moment,
    },
    /// Reports driver ownership of the current coordinator call.
    DriverAccepted,
    /// Reports definite rejection before driver ownership.
    DriverRejected,
    /// Reports original-deadline expiry before driver ownership.
    DeadlineElapsed,
    /// Reports original-deadline expiry after driver ownership.
    DriverDeadlineElapsed {
        /// Driver-authoritative delivery certainty.
        delivery: DeliveryStatus,
    },
    /// Reports one correlated per-group outcome.
    BrokerResponded {
        /// Nonnegative broker throttle observation.
        throttle_time_ms: u32,
        /// Exact correlated per-group outcome.
        outcome: AdminConsumerGroupDescriptionOutcome,
    },
    /// Reports that the modern attempt permits one classic fallback.
    FallbackToClassic {
        /// Nonnegative throttle observed from a modern broker response, or zero
        /// for a local compatibility terminal.
        throttle_time_ms: u32,
        /// Delivery certainty for the modern attempt.
        delivery: DeliveryStatus,
    },
    /// Reports a structurally valid response exceeding retained capacity.
    ResponseTooLarge,
    /// Reports insufficient negotiated protocol semantics.
    ProtocolIncompatible {
        /// Authoritative certainty at incompatibility discovery.
        delivery: DeliveryStatus,
    },
    /// Reports driver-owned transport failure.
    TransportFailed {
        /// Driver-authoritative delivery certainty.
        delivery: DeliveryStatus,
    },
    /// Reports malformed or uncorrelatable response data.
    InvalidResponse,
}

/// One concrete mechanism request emitted by core policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdminDescribeConsumerGroupsEffect {
    /// Materializes and coordinator-routes one singleton group request.
    Submit {
        /// Stable identity reserved before machine construction.
        operation_id: OperationId,
        /// Original public absolute deadline.
        deadline: Deadline,
        /// Exact current coordinator key.
        group_id: String,
        /// Exact authorization-bit expansion intent.
        include_authorized_operations: bool,
        /// Concrete modern-first or classic-fallback protocol.
        call_kind: AdminDescribeConsumerGroupsCallKind,
    },
    /// Publishes the sole terminal decision.
    Complete {
        /// Stable operation identity.
        operation_id: OperationId,
        /// Core-owned terminal decision.
        terminal: AdminDescribeConsumerGroupsTerminal,
    },
}

/// Ordered result of one deterministic state transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminDescribeConsumerGroupsTransition {
    effect: Option<AdminDescribeConsumerGroupsEffect>,
}

impl AdminDescribeConsumerGroupsTransition {
    pub(crate) const fn none() -> Self {
        Self { effect: None }
    }

    pub(crate) const fn one(effect: AdminDescribeConsumerGroupsEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Consumes this transition into its optional effect.
    pub fn into_effect(self) -> Option<AdminDescribeConsumerGroupsEffect> {
        self.effect
    }
}

/// Deterministic owner for one capacity-reserved description batch.
#[derive(Debug)]
pub struct AdminDescribeConsumerGroupsMachine {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: Deadline,
    pub(crate) plan: AdminDescribeConsumerGroupsPlan,
    pub(crate) state: AdminDescribeConsumerGroupsState,
    pub(crate) next_group: usize,
    pub(crate) maximum_throttle_time_ms: u32,
    pub(crate) outcomes: Vec<AdminConsumerGroupDescriptionOutcome>,
    pub(crate) call_kind: AdminDescribeConsumerGroupsCallKind,
    pub(crate) prior_delivery: DeliveryStatus,
}

impl AdminDescribeConsumerGroupsMachine {
    /// Creates one accepted operation after engine capacity reservation.
    pub fn new(
        operation_id: OperationId,
        deadline: Deadline,
        plan: AdminDescribeConsumerGroupsPlan,
    ) -> Self {
        let outcomes = Vec::with_capacity(plan.groups().len());
        Self {
            operation_id,
            deadline,
            plan,
            state: AdminDescribeConsumerGroupsState::Ready,
            next_group: 0,
            maximum_throttle_time_ms: 0,
            outcomes,
            call_kind: AdminDescribeConsumerGroupsCallKind::Consumer,
            prior_delivery: DeliveryStatus::NotSent,
        }
    }

    /// Returns the current lifecycle stage.
    pub const fn state(&self) -> AdminDescribeConsumerGroupsState {
        self.state
    }

    /// Returns the exact group currently awaiting or owned by the driver.
    pub fn current_group(&self) -> Option<&str> {
        self.plan.groups().get(self.next_group).map(String::as_str)
    }

    /// Returns authorization expansion intent retained for the current call.
    pub const fn include_authorized_operations(&self) -> bool {
        self.plan.include_authorized_operations()
    }

    /// Returns the concrete protocol owned for the current group attempt.
    pub const fn call_kind(&self) -> AdminDescribeConsumerGroupsCallKind {
        self.call_kind
    }
}

/// Rejected state-machine fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminDescribeConsumerGroupsMachineError {
    /// The fact does not belong to the current lifecycle stage.
    InvalidState,
    /// The operation already owns its terminal.
    AlreadyCompleted,
}

impl fmt::Display for AdminDescribeConsumerGroupsMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "DescribeConsumerGroups machine rejected fact: {self:?}"
        )
    }
}

impl std::error::Error for AdminDescribeConsumerGroupsMachineError {}
