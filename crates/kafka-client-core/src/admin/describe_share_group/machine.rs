//! Single-owner lifecycle vocabulary for caller-ordered API-77 group description.

use core::fmt;
use core::mem::size_of;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    DescribeShareGroupBrokerError, DescribeShareGroupFailure, DescribeShareGroupPlan,
    DescribeShareGroupPlanShape, DescribeShareGroupResult,
};

/// Exact result for one share group in a caller-ordered batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DescribeShareGroupOutcome {
    /// Kafka returned one exact correlated group description.
    Described(DescribeShareGroupResult),
    /// Kafka rejected this specific share group.
    BrokerRejected {
        /// Exact requested share-group identity.
        group_id: String,
        /// Exact signed rejection, throttle, and bounded diagnostic.
        error: DescribeShareGroupBrokerError,
    },
}

impl DescribeShareGroupOutcome {
    /// Creates one successful per-group outcome.
    pub const fn described(result: DescribeShareGroupResult) -> Self {
        Self::Described(result)
    }

    /// Creates one rejected per-group outcome.
    pub const fn broker_rejected(group_id: String, error: DescribeShareGroupBrokerError) -> Self {
        Self::BrokerRejected { group_id, error }
    }

    /// Returns the exact requested share-group identity.
    pub fn group_id(&self) -> &str {
        match self {
            Self::Described(result) => result.description().group_id(),
            Self::BrokerRejected { group_id, .. } => group_id,
        }
    }

    /// Returns this group's nonnegative broker throttle observation.
    pub const fn throttle_time_ms(&self) -> u32 {
        match self {
            Self::Described(result) => result.throttle_time_ms(),
            Self::BrokerRejected { error, .. } => error.throttle_time_ms(),
        }
    }
}

/// Caller-ordered outcomes for one batch operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeShareGroupsBatch {
    throttle_time_ms: u32,
    outcomes: Vec<DescribeShareGroupOutcome>,
}

impl DescribeShareGroupsBatch {
    /// Creates one normalized batch with the maximum observed broker throttle.
    pub const fn new(throttle_time_ms: u32, outcomes: Vec<DescribeShareGroupOutcome>) -> Self {
        Self {
            throttle_time_ms,
            outcomes,
        }
    }

    /// Returns the maximum nonnegative throttle observed across coordinator calls.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns exactly one outcome per requested group in caller order.
    pub fn outcomes(&self) -> &[DescribeShareGroupOutcome] {
        &self.outcomes
    }

    /// Consumes this batch into adapter-owned parts.
    pub fn into_parts(self) -> (u32, Vec<DescribeShareGroupOutcome>) {
        (self.throttle_time_ms, self.outcomes)
    }
}

/// Exactly one terminal decision for one API-77 operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DescribeShareGroupTerminal {
    /// Kafka returned one exact correlated group description.
    Described(DescribeShareGroupResult),
    /// Kafka rejected the requested group.
    BrokerRejected(DescribeShareGroupBrokerError),
    /// Every requested group settled in original caller order.
    Batch(DescribeShareGroupsBatch),
    /// Execution failed outside an exact Kafka rejection.
    Failed(DescribeShareGroupFailure),
}

/// Current ownership stage for one API-77 operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeShareGroupState {
    /// Accepted but not yet offered to the driver.
    Ready,
    /// The exact semantic plan awaits driver admission.
    AwaitingDriver,
    /// The driver owns the current read-only RPC attempt.
    Submitted,
    /// Core assigned the sole terminal outcome.
    Completed,
}

/// One normalized fact applied to caller-ordered API-77 policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DescribeShareGroupInput {
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
        result: DescribeShareGroupResult,
    },
    /// Reports Kafka's exact signed group rejection.
    BrokerRejected {
        /// Exact rejection, throttle, and bounded diagnostic.
        error: DescribeShareGroupBrokerError,
    },
    /// Reports a structurally valid response exceeding retained capacity.
    ResponseTooLarge,
    /// Reports that stable API-77 v1 is unavailable.
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

/// One concrete mechanism request emitted by caller-ordered API-77 policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DescribeShareGroupEffect {
    /// Submit one validated group through its coordinator.
    Submit {
        /// Stable identity reserved before machine construction.
        operation_id: OperationId,
        /// Original public absolute deadline.
        deadline: Deadline,
        /// One-element semantic request projection.
        plan: DescribeShareGroupPlan,
    },
    /// Publish the one terminal decision.
    Complete {
        /// Stable operation identity.
        operation_id: OperationId,
        /// Core-owned terminal decision.
        terminal: DescribeShareGroupTerminal,
    },
}

/// Ordered result of one deterministic transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeShareGroupTransition {
    effect: Option<DescribeShareGroupEffect>,
}

impl DescribeShareGroupTransition {
    pub(crate) const fn none() -> Self {
        Self { effect: None }
    }

    pub(crate) const fn one(effect: DescribeShareGroupEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Consumes this transition into its optional effect.
    pub fn into_effect(self) -> Option<DescribeShareGroupEffect> {
        self.effect
    }
}

/// Deterministic owner for one capacity-reserved API-77 operation.
#[derive(Debug)]
pub struct DescribeShareGroupMachine {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: Deadline,
    pub(crate) plan: DescribeShareGroupPlan,
    pub(crate) state: DescribeShareGroupState,
    pub(crate) next_group: usize,
    pub(crate) maximum_throttle_time_ms: u32,
    pub(crate) outcomes: Vec<DescribeShareGroupOutcome>,
    pub(crate) response_text_bytes: usize,
    pub(crate) response_retained_bytes: usize,
}

impl DescribeShareGroupMachine {
    /// Creates one accepted operation after engine terminal and byte reservation.
    pub fn new(
        operation_id: OperationId,
        deadline: Deadline,
        plan: DescribeShareGroupPlan,
    ) -> Self {
        let (outcomes, response_retained_bytes) = match plan.shape() {
            DescribeShareGroupPlanShape::Singular => {
                (Vec::new(), size_of::<DescribeShareGroupResult>())
            }
            DescribeShareGroupPlanShape::Batch => {
                let outcomes = Vec::with_capacity(plan.group_ids().len());
                let retained = size_of::<DescribeShareGroupsBatch>()
                    + plan.group_ids().len() * size_of::<DescribeShareGroupOutcome>();
                (outcomes, retained)
            }
        };
        Self {
            operation_id,
            deadline,
            plan,
            state: DescribeShareGroupState::Ready,
            next_group: 0,
            maximum_throttle_time_ms: 0,
            outcomes,
            response_text_bytes: 0,
            response_retained_bytes,
        }
    }

    /// Returns the current lifecycle stage.
    pub const fn state(&self) -> DescribeShareGroupState {
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

/// Rejected API-77 state-machine fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeShareGroupMachineError {
    /// The fact does not belong to the current lifecycle stage.
    InvalidState,
    /// The operation already owns its terminal decision.
    AlreadyCompleted,
}

impl fmt::Display for DescribeShareGroupMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "DescribeShareGroup machine rejected fact: {self:?}"
        )
    }
}

impl std::error::Error for DescribeShareGroupMachineError {}
