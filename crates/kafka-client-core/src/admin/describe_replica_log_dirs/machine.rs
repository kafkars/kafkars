//! Sequential exact-broker lifecycle vocabulary for replica placement description.

use core::fmt;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    DescribeReplicaLogDirsBatch, DescribeReplicaLogDirsBrokerError, DescribeReplicaLogDirsFailure,
    DescribeReplicaLogDirsPlan, DescribeReplicaLogDirsReplica,
    DescribeReplicaLogDirsReplicaOutcome, DescribeReplicaLogDirsReplicaPlacement,
    DescribeReplicaLogDirsTerminal,
};

/// Current ownership stage for one replica log-directory operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeReplicaLogDirsState {
    /// Accepted but not started.
    Ready,
    /// One exact broker call awaits driver admission.
    AwaitingDriver,
    /// The driver owns one exact broker call.
    Submitted,
    /// Core assigned the sole terminal decision.
    Completed,
}

/// One normalized fact applied to deterministic replica placement policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DescribeReplicaLogDirsInput {
    /// Starts execution at the supplied monotonic observation.
    Start {
        /// Current monotonic observation.
        now: Moment,
    },
    /// Reports driver ownership of the current exact-broker call.
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
    /// Reports one normalized response from an exact broker.
    BrokerResponded {
        /// Exact broker addressed by this response.
        broker_id: i32,
        /// Nonnegative broker throttle observation.
        throttle_time_ms: u32,
        /// Placements in relative request order, or one top-level broker error.
        result:
            Result<Vec<DescribeReplicaLogDirsReplicaPlacement>, DescribeReplicaLogDirsBrokerError>,
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
pub enum DescribeReplicaLogDirsEffect {
    /// Submit one selected-replica query to an exact broker.
    Submit {
        /// Stable identity reserved before machine construction.
        operation_id: OperationId,
        /// Original public absolute deadline.
        deadline: Deadline,
        /// Exact requested broker identity.
        broker_id: i32,
        /// This broker's replicas in relative caller order.
        replicas: Vec<DescribeReplicaLogDirsReplica>,
    },
    /// Publish the sole terminal decision.
    Complete {
        /// Stable operation identity.
        operation_id: OperationId,
        /// Core-owned terminal decision.
        terminal: DescribeReplicaLogDirsTerminal,
    },
}

/// Ordered result of one deterministic state transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeReplicaLogDirsTransition {
    effect: Option<DescribeReplicaLogDirsEffect>,
}

impl DescribeReplicaLogDirsTransition {
    pub(crate) const fn none() -> Self {
        Self { effect: None }
    }

    pub(crate) const fn one(effect: DescribeReplicaLogDirsEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Consumes this transition into its optional effect.
    pub fn into_effect(self) -> Option<DescribeReplicaLogDirsEffect> {
        self.effect
    }
}

/// Deterministic owner for one capacity-reserved replica description.
#[derive(Debug)]
pub struct DescribeReplicaLogDirsMachine {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: Deadline,
    pub(crate) plan: DescribeReplicaLogDirsPlan,
    pub(crate) state: DescribeReplicaLogDirsState,
    pub(crate) next_broker: usize,
    pub(crate) maximum_throttle_time_ms: u32,
    pub(crate) outcomes: Vec<Option<DescribeReplicaLogDirsReplicaOutcome>>,
}

impl DescribeReplicaLogDirsMachine {
    /// Creates one accepted operation after terminal and byte reservation.
    pub fn new(
        operation_id: OperationId,
        deadline: Deadline,
        plan: DescribeReplicaLogDirsPlan,
    ) -> Self {
        let outcomes = (0..plan.replicas().len()).map(|_| None).collect();
        Self {
            operation_id,
            deadline,
            plan,
            state: DescribeReplicaLogDirsState::Ready,
            next_broker: 0,
            maximum_throttle_time_ms: 0,
            outcomes,
        }
    }

    /// Returns the current lifecycle stage.
    pub const fn state(&self) -> DescribeReplicaLogDirsState {
        self.state
    }

    /// Returns the exact broker currently awaiting or owned by the driver.
    pub fn current_broker(&self) -> Option<i32> {
        self.plan.broker_ids().get(self.next_broker).copied()
    }
}

/// Rejected deterministic replica-description state-machine fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeReplicaLogDirsMachineError {
    /// The fact does not belong to the current lifecycle stage.
    InvalidState,
    /// The operation already owns its terminal.
    AlreadyCompleted,
    /// Internal settlement did not fill every caller result slot.
    IncompleteOutcome,
}

impl fmt::Display for DescribeReplicaLogDirsMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "DescribeReplicaLogDirs machine rejected fact: {self:?}"
        )
    }
}

impl std::error::Error for DescribeReplicaLogDirsMachineError {}

impl DescribeReplicaLogDirsMachine {
    pub(crate) fn completed_transition(
        &mut self,
    ) -> Result<DescribeReplicaLogDirsTransition, DescribeReplicaLogDirsMachineError> {
        if self.outcomes.iter().any(Option::is_none) {
            return Err(DescribeReplicaLogDirsMachineError::IncompleteOutcome);
        }
        let outcomes = core::mem::take(&mut self.outcomes)
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .ok_or(DescribeReplicaLogDirsMachineError::IncompleteOutcome)?;
        self.state = DescribeReplicaLogDirsState::Completed;
        Ok(DescribeReplicaLogDirsTransition::one(
            DescribeReplicaLogDirsEffect::Complete {
                operation_id: self.operation_id,
                terminal: DescribeReplicaLogDirsTerminal::Described(
                    DescribeReplicaLogDirsBatch::new(self.maximum_throttle_time_ms, outcomes),
                ),
            },
        ))
    }

    pub(crate) fn failure(
        kind: super::DescribeReplicaLogDirsFailureKind,
        delivery: DeliveryStatus,
    ) -> DescribeReplicaLogDirsFailure {
        DescribeReplicaLogDirsFailure::new(kind, delivery)
    }
}
