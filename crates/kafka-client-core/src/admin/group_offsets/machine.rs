//! Single-owner lifecycle vocabulary for singular and batched group-offset queries.

use core::{fmt, mem::size_of, num::NonZeroI16};

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    ListConsumerGroupOffsetsBatch, ListConsumerGroupOffsetsPlan, ListConsumerGroupOffsetsTerminal,
    model::ListConsumerGroupOffsetsPlanShape, outcome::ListConsumerGroupBatchOutcome,
};

/// Current ownership stage for one consumer-group offset query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListConsumerGroupOffsetsState {
    /// Accepted but not yet offered to the driver.
    Ready,
    /// The exact semantic plan awaits driver admission.
    AwaitingDriver,
    /// The driver owns the sole RPC attempt.
    Submitted,
    /// Core assigned the sole terminal outcome.
    Completed,
}

/// One normalized fact applied to group-offset listing policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListConsumerGroupOffsetsInput {
    /// Starts execution at the supplied monotonic observation.
    Start {
        /// Current monotonic observation.
        now: Moment,
    },
    /// Reports that the driver accepted the request.
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
    /// Reports ordered protocol-normalized partition outcomes.
    BrokerResponded {
        /// Nonnegative throttle and outcomes in deterministic order.
        batch: ListConsumerGroupOffsetsBatch,
    },
    /// Reports Kafka's exact top-level group error.
    BrokerRejected {
        /// Kafka's exact nonzero signed error code.
        code: NonZeroI16,
        /// Nonnegative throttle observed for this group response.
        throttle_time_ms: u32,
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

/// One concrete mechanism request emitted by group-offset policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListConsumerGroupOffsetsEffect {
    /// Materialize and submit the validated plan with its original deadline.
    Submit {
        /// Stable identity reserved before machine construction.
        operation_id: OperationId,
        /// Original public absolute deadline.
        deadline: Deadline,
        /// Exact semantic request intent.
        plan: ListConsumerGroupOffsetsPlan,
    },
    /// Publish the one terminal decision.
    Complete {
        /// Stable operation identity.
        operation_id: OperationId,
        /// Core-owned terminal decision.
        terminal: ListConsumerGroupOffsetsTerminal,
    },
}

/// Ordered result of one deterministic state transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListConsumerGroupOffsetsTransition {
    effect: Option<ListConsumerGroupOffsetsEffect>,
}

impl ListConsumerGroupOffsetsTransition {
    pub(crate) const fn none() -> Self {
        Self { effect: None }
    }

    pub(crate) const fn one(effect: ListConsumerGroupOffsetsEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Consumes this transition into its optional effect.
    pub fn into_effect(self) -> Option<ListConsumerGroupOffsetsEffect> {
        self.effect
    }
}

/// Deterministic owner for one capacity-reserved group-offset query.
#[derive(Debug)]
pub struct ListConsumerGroupOffsetsMachine {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: Deadline,
    pub(crate) plan: ListConsumerGroupOffsetsPlan,
    pub(crate) state: ListConsumerGroupOffsetsState,
    pub(crate) next_group: usize,
    pub(crate) maximum_throttle_time_ms: u32,
    pub(crate) outcomes: Vec<ListConsumerGroupBatchOutcome>,
}

impl ListConsumerGroupOffsetsMachine {
    /// Creates one accepted operation after engine terminal reservation.
    pub fn new(
        operation_id: OperationId,
        deadline: Deadline,
        plan: ListConsumerGroupOffsetsPlan,
    ) -> Self {
        let outcomes = match plan.shape() {
            ListConsumerGroupOffsetsPlanShape::Singular => Vec::new(),
            ListConsumerGroupOffsetsPlanShape::Batch => {
                let capacity = plan.group_ids().len();
                debug_assert!(
                    capacity
                        .checked_mul(size_of::<ListConsumerGroupBatchOutcome>())
                        .is_some()
                );
                Vec::with_capacity(capacity)
            }
        };
        Self {
            operation_id,
            deadline,
            plan,
            state: ListConsumerGroupOffsetsState::Ready,
            next_group: 0,
            maximum_throttle_time_ms: 0,
            outcomes,
        }
    }

    /// Returns the current lifecycle stage.
    pub const fn state(&self) -> ListConsumerGroupOffsetsState {
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

/// Rejected group-offset state-machine fact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListConsumerGroupOffsetsMachineError {
    /// The fact does not belong to the current lifecycle stage.
    InvalidState,
    /// The operation already owns its terminal decision.
    AlreadyCompleted,
}

impl fmt::Display for ListConsumerGroupOffsetsMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "ListConsumerGroupOffsets machine rejected fact: {self:?}"
        )
    }
}

impl std::error::Error for ListConsumerGroupOffsetsMachineError {}
