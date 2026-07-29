//! Single-owner lifecycle vocabulary for singular and batched share-group offset listing.

use core::{fmt, mem::size_of};

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    ListShareGroupOffsetsBatch, ListShareGroupOffsetsBatchOutcome,
    ListShareGroupOffsetsBrokerError, ListShareGroupOffsetsPlan, ListShareGroupOffsetsPlanShape,
    ListShareGroupOffsetsTerminal, ListShareGroupsOffsetsBatch,
};

/// Current ownership stage for one accepted API-90 operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ListShareGroupOffsetsState {
    /// Accepted but not yet offered to the driver.
    Ready,
    /// The exact semantic plan awaits driver admission.
    AwaitingDriver,
    /// The driver owns the current read-only RPC attempt.
    Submitted,
    /// Core assigned the sole terminal outcome.
    Completed,
}

/// One normalized fact applied to share-group offset listing policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ListShareGroupOffsetsInput {
    /// Starts execution at the supplied monotonic observation.
    Start {
        /// Current monotonic observation.
        now: Moment,
    },
    /// Reports that the driver accepted the exact request.
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
    /// Reports one protocol-normalized response for the current group.
    BrokerResponded {
        /// Nonnegative throttle and flattened partition outcomes.
        batch: ListShareGroupOffsetsBatch,
    },
    /// Reports Kafka's exact signed group-level rejection.
    BrokerRejected {
        /// Exact rejection, throttle, and bounded nullable diagnostic.
        error: ListShareGroupOffsetsBrokerError,
    },
    /// Reports a structurally valid response exceeding retained capacity.
    ResponseTooLarge,
    /// Reports that the selected version cannot represent required semantics.
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

/// One concrete mechanism request emitted by API-90 policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ListShareGroupOffsetsEffect {
    /// Submit the current one-group projection through its exact coordinator.
    Submit {
        /// Stable identity reserved before machine construction.
        operation_id: OperationId,
        /// Original public absolute deadline.
        deadline: Deadline,
        /// Exact one-group all-or-selected semantic request intent.
        plan: ListShareGroupOffsetsPlan,
    },
    /// Publish the one terminal decision.
    Complete {
        /// Stable operation identity.
        operation_id: OperationId,
        /// Core-owned terminal decision.
        terminal: ListShareGroupOffsetsTerminal,
    },
}

/// Ordered result of one deterministic state transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListShareGroupOffsetsTransition {
    effect: Option<ListShareGroupOffsetsEffect>,
}

impl ListShareGroupOffsetsTransition {
    pub(crate) const fn none() -> Self {
        Self { effect: None }
    }

    pub(crate) const fn one(effect: ListShareGroupOffsetsEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Consumes this transition into its optional effect.
    pub fn into_effect(self) -> Option<ListShareGroupOffsetsEffect> {
        self.effect
    }
}

/// Deterministic owner for one capacity-reserved API-90 operation.
#[derive(Debug)]
pub struct ListShareGroupOffsetsMachine {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: Deadline,
    pub(crate) plan: ListShareGroupOffsetsPlan,
    pub(crate) state: ListShareGroupOffsetsState,
    pub(crate) next_group: usize,
    pub(crate) maximum_throttle_time_ms: u32,
    pub(crate) outcomes: Vec<ListShareGroupOffsetsBatchOutcome>,
    pub(crate) response_text_bytes: usize,
    pub(crate) response_retained_bytes: usize,
}

impl ListShareGroupOffsetsMachine {
    /// Creates one accepted operation after engine terminal reservation.
    pub fn new(
        operation_id: OperationId,
        deadline: Deadline,
        plan: ListShareGroupOffsetsPlan,
    ) -> Self {
        let (outcomes, response_retained_bytes) = match plan.shape() {
            ListShareGroupOffsetsPlanShape::Singular => {
                (Vec::new(), size_of::<ListShareGroupOffsetsBatch>())
            }
            ListShareGroupOffsetsPlanShape::Batch => {
                let outcomes = Vec::with_capacity(plan.queries().len());
                let retained = size_of::<ListShareGroupsOffsetsBatch>()
                    + plan.queries().len() * size_of::<ListShareGroupOffsetsBatchOutcome>();
                (outcomes, retained)
            }
        };
        Self {
            operation_id,
            deadline,
            plan,
            state: ListShareGroupOffsetsState::Ready,
            next_group: 0,
            maximum_throttle_time_ms: 0,
            outcomes,
            response_text_bytes: 0,
            response_retained_bytes,
        }
    }

    /// Returns the current lifecycle stage.
    pub const fn state(&self) -> ListShareGroupOffsetsState {
        self.state
    }

    /// Returns the exact group currently awaiting or owned by the driver.
    pub fn current_group_id(&self) -> Option<&str> {
        self.plan
            .queries()
            .get(self.next_group)
            .map(|query| query.group_id())
    }
}

/// Rejected API-90 state-machine fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ListShareGroupOffsetsMachineError {
    /// The fact does not belong to the current lifecycle stage.
    InvalidState,
    /// The operation already owns its terminal decision.
    AlreadyCompleted,
}

impl fmt::Display for ListShareGroupOffsetsMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "ListShareGroupOffsets rejected fact: {self:?}")
    }
}

impl std::error::Error for ListShareGroupOffsetsMachineError {}
