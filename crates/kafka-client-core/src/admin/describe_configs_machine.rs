//! Single-owner lifecycle vocabulary for one bounded `DescribeConfigs` batch.

use core::fmt;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{DescribeConfigsBatch, DescribeConfigsPlan, DescribeConfigsTerminal};

/// Current ownership stage for one `DescribeConfigs` operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DescribeConfigsState {
    /// Accepted but not yet offered to the driver.
    Ready,
    /// The exact request plan awaits driver admission.
    AwaitingDriver,
    /// The driver owns the RPC attempt.
    Submitted,
    /// Core assigned the sole terminal outcome.
    Completed,
}

/// One normalized fact applied to `DescribeConfigs` policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DescribeConfigsInput {
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
    /// Reports deadline expiry after driver ownership.
    DriverDeadlineElapsed {
        /// Driver-authoritative delivery certainty.
        delivery: DeliveryStatus,
    },
    /// Reports one ordered, bounded, protocol-normalized response.
    BrokerResponded {
        /// Resource results plus Kafka's throttle observation.
        batch: DescribeConfigsBatch,
    },
    /// Reports a structurally valid response exceeding admitted result capacity.
    ResponseTooLarge,
    /// Reports that the selected API version cannot represent the request or response.
    ProtocolIncompatible {
        /// Authoritative certainty at the point incompatibility was discovered.
        delivery: DeliveryStatus,
    },
    /// Reports a driver-owned transport terminal.
    TransportFailed {
        /// Driver-authoritative delivery certainty.
        delivery: DeliveryStatus,
    },
    /// Reports a malformed or uncorrelated broker response.
    InvalidResponse,
}

/// One concrete mechanism request emitted by `DescribeConfigs` policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DescribeConfigsEffect {
    /// Materialize and submit the validated plan with its original deadline.
    Submit {
        /// Stable identity reserved before machine construction.
        operation_id: OperationId,
        /// Original public absolute deadline.
        deadline: Deadline,
        /// Ordered semantic request facts.
        plan: DescribeConfigsPlan,
    },
    /// Publish the one terminal decision.
    Complete {
        /// Stable operation identity.
        operation_id: OperationId,
        /// Core-owned terminal decision.
        terminal: DescribeConfigsTerminal,
    },
}

/// Ordered result of one `DescribeConfigs` state transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeConfigsTransition {
    effect: Option<DescribeConfigsEffect>,
}

impl DescribeConfigsTransition {
    pub(crate) const fn none() -> Self {
        Self { effect: None }
    }

    pub(crate) const fn one(effect: DescribeConfigsEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Consumes this transition into its optional effect.
    pub fn into_effect(self) -> Option<DescribeConfigsEffect> {
        self.effect
    }
}

/// Deterministic owner for one capacity-reserved `DescribeConfigs` operation.
#[derive(Debug)]
pub struct DescribeConfigsMachine {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: Deadline,
    pub(crate) plan: DescribeConfigsPlan,
    pub(crate) state: DescribeConfigsState,
}

impl DescribeConfigsMachine {
    /// Creates one accepted operation after engine terminal reservation.
    pub const fn new(
        operation_id: OperationId,
        deadline: Deadline,
        plan: DescribeConfigsPlan,
    ) -> Self {
        Self {
            operation_id,
            deadline,
            plan,
            state: DescribeConfigsState::Ready,
        }
    }

    /// Returns the current lifecycle stage.
    pub const fn state(&self) -> DescribeConfigsState {
        self.state
    }
}

/// Rejected `DescribeConfigs` state-machine fact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DescribeConfigsMachineError {
    /// The fact does not belong to the current lifecycle stage.
    InvalidState,
    /// The operation already owns its terminal decision.
    AlreadyCompleted,
    /// The normalized response has a different resource count.
    OutcomeCountMismatch,
    /// A normalized response is not in original resource order.
    OutcomeResourceMismatch,
    /// Successful configuration entries do not match their query selection.
    ConfigurationCorrelationMismatch,
}

impl fmt::Display for DescribeConfigsMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "DescribeConfigs machine rejected fact: {self:?}")
    }
}

impl std::error::Error for DescribeConfigsMachineError {}
