//! Single-owner lifecycle vocabulary for caller-ordered API-89 group description.

use core::fmt;
use core::mem::size_of;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    DescribeStreamsGroupBrokerError, DescribeStreamsGroupOutcome, DescribeStreamsGroupPlan,
    DescribeStreamsGroupPlanShape, DescribeStreamsGroupResult, DescribeStreamsGroupTerminal,
    DescribeStreamsGroupsBatch,
};

/// Current ownership stage for one API-89 operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeStreamsGroupState {
    /// Accepted but not yet offered to the driver.
    Ready,
    /// The current one-group semantic projection awaits driver admission.
    AwaitingDriver,
    /// The driver owns the current read-only RPC attempt.
    Submitted,
    /// Core assigned the sole terminal outcome.
    Completed,
}

/// One normalized fact applied to caller-ordered API-89 policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DescribeStreamsGroupInput {
    /// Starts execution at the supplied monotonic observation.
    Start {
        /// Current monotonic observation.
        now: Moment,
    },
    /// Reports that the driver accepted the current exact request.
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
    /// Reports a protocol-normalized successful response.
    BrokerResponded {
        /// Nonnegative throttle and exact group description.
        result: DescribeStreamsGroupResult,
    },
    /// Reports Kafka's exact signed group rejection.
    BrokerRejected {
        /// Exact rejection, throttle, and bounded diagnostic.
        error: DescribeStreamsGroupBrokerError,
    },
    /// Reports a structurally valid response exceeding retained capacity.
    ResponseTooLarge,
    /// Reports that stable API-89 v1 is unavailable.
    ProtocolIncompatible {
        /// Authoritative certainty at incompatibility discovery.
        delivery: DeliveryStatus,
    },
    /// Reports a driver-owned transport terminal.
    TransportFailed {
        /// Driver-authoritative delivery certainty.
        delivery: DeliveryStatus,
    },
    /// Reports a broker response that cannot be normalized.
    InvalidResponse,
}

/// One concrete mechanism request emitted by caller-ordered API-89 policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DescribeStreamsGroupEffect {
    /// Submit one validated group through its coordinator.
    Submit {
        /// Stable identity reserved before machine construction.
        operation_id: OperationId,
        /// Original public absolute deadline.
        deadline: Deadline,
        /// One-element semantic request projection.
        plan: DescribeStreamsGroupPlan,
    },
    /// Publish the one terminal decision.
    Complete {
        /// Stable operation identity.
        operation_id: OperationId,
        /// Core-owned terminal decision.
        terminal: DescribeStreamsGroupTerminal,
    },
}

/// Ordered result of one deterministic transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeStreamsGroupTransition {
    effect: Option<DescribeStreamsGroupEffect>,
}

impl DescribeStreamsGroupTransition {
    pub(crate) const fn none() -> Self {
        Self { effect: None }
    }

    pub(crate) const fn one(effect: DescribeStreamsGroupEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Consumes this transition into its optional effect.
    pub fn into_effect(self) -> Option<DescribeStreamsGroupEffect> {
        self.effect
    }
}

/// Deterministic owner for one capacity-reserved API-89 operation.
#[derive(Debug)]
pub struct DescribeStreamsGroupMachine {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: Deadline,
    pub(crate) plan: DescribeStreamsGroupPlan,
    pub(crate) state: DescribeStreamsGroupState,
    pub(crate) next_group: usize,
    pub(crate) maximum_throttle_time_ms: u32,
    pub(crate) outcomes: Vec<DescribeStreamsGroupOutcome>,
    pub(crate) response_text_bytes: usize,
    pub(crate) response_retained_bytes: usize,
}

impl DescribeStreamsGroupMachine {
    /// Creates one accepted operation after engine terminal and byte reservation.
    pub fn new(
        operation_id: OperationId,
        deadline: Deadline,
        plan: DescribeStreamsGroupPlan,
    ) -> Self {
        let outcomes = Vec::with_capacity(plan.group_ids().len());
        let response_retained_bytes = match plan.shape() {
            DescribeStreamsGroupPlanShape::Singular => size_of::<DescribeStreamsGroupResult>(),
            DescribeStreamsGroupPlanShape::Batch => {
                size_of::<DescribeStreamsGroupsBatch>()
                    + plan.group_ids().len() * size_of::<DescribeStreamsGroupOutcome>()
            }
        };
        Self {
            operation_id,
            deadline,
            plan,
            state: DescribeStreamsGroupState::Ready,
            next_group: 0,
            maximum_throttle_time_ms: 0,
            outcomes,
            response_text_bytes: 0,
            response_retained_bytes,
        }
    }

    /// Returns the current lifecycle stage.
    pub const fn state(&self) -> DescribeStreamsGroupState {
        self.state
    }

    /// Returns the exact group currently awaiting or owned by the driver.
    pub fn current_group_id(&self) -> Option<&str> {
        self.plan
            .group_ids()
            .get(self.next_group)
            .map(String::as_str)
    }
}

/// Rejected API-89 state-machine fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeStreamsGroupMachineError {
    /// The fact does not belong to the current lifecycle stage.
    InvalidState,
    /// The operation already owns its terminal decision.
    AlreadyCompleted,
}

impl fmt::Display for DescribeStreamsGroupMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "DescribeStreamsGroup machine rejected fact: {self:?}"
        )
    }
}

impl std::error::Error for DescribeStreamsGroupMachineError {}
