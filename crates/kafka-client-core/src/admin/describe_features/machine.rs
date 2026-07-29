//! Single-owner lifecycle vocabulary for one fixed feature-metadata query.

use core::fmt;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{DescribeFeaturesBrokerError, DescribeFeaturesDescription, DescribeFeaturesTerminal};

/// Current ownership stage for one feature-metadata query.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeFeaturesState {
    /// Accepted after completion and retained-byte capacity was reserved.
    Ready,
    /// The sole fixed request awaits driver admission.
    AwaitingDriver,
    /// The driver owns the sole request attempt.
    Submitted,
    /// Core assigned the sole terminal decision.
    Completed,
}

/// One normalized fact applied to feature-metadata policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DescribeFeaturesInput {
    /// Starts the operation at one supplied monotonic observation.
    Start {
        /// Current monotonic observation.
        now: Moment,
    },
    /// Reports driver ownership of the sole request.
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
    /// Reports one validated, bounded, protocol-normalized API-18 response.
    BrokerResponded {
        /// Wire-free feature metadata.
        description: DescribeFeaturesDescription,
    },
    /// Reports Kafka's exact top-level API-18 rejection.
    BrokerRejected {
        /// Exact signed broker code and throttle observation.
        error: DescribeFeaturesBrokerError,
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
    /// Reports malformed or contradictory response data.
    InvalidResponse,
}

/// One concrete mechanism request emitted by deterministic feature policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DescribeFeaturesEffect {
    /// Submit the fixed empty request exactly once.
    Submit {
        /// Stable identity reserved before machine construction.
        operation_id: OperationId,
        /// Original public absolute deadline.
        deadline: Deadline,
    },
    /// Publish the sole terminal decision.
    Complete {
        /// Stable operation identity.
        operation_id: OperationId,
        /// Core-owned terminal decision.
        terminal: DescribeFeaturesTerminal,
    },
}

/// Ordered result of one deterministic state transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeFeaturesTransition {
    effect: Option<DescribeFeaturesEffect>,
}

impl DescribeFeaturesTransition {
    pub(crate) const fn none() -> Self {
        Self { effect: None }
    }

    pub(crate) const fn one(effect: DescribeFeaturesEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Consumes this transition into its optional concrete effect.
    pub fn into_effect(self) -> Option<DescribeFeaturesEffect> {
        self.effect
    }
}

/// Deterministic owner for one capacity-reserved `DescribeFeatures` query.
#[derive(Debug)]
pub struct DescribeFeaturesMachine {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: Deadline,
    pub(crate) state: DescribeFeaturesState,
}

impl DescribeFeaturesMachine {
    /// Creates one accepted query after terminal and retained-byte reservation.
    pub const fn new(operation_id: OperationId, deadline: Deadline) -> Self {
        Self {
            operation_id,
            deadline,
            state: DescribeFeaturesState::Ready,
        }
    }

    /// Returns the current lifecycle stage.
    pub const fn state(&self) -> DescribeFeaturesState {
        self.state
    }
}

/// Rejected deterministic feature-metadata fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeFeaturesMachineError {
    /// The fact does not belong to the current lifecycle stage.
    InvalidState,
    /// The operation already owns its terminal decision.
    AlreadyCompleted,
}

impl fmt::Display for DescribeFeaturesMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "DescribeFeatures machine rejected fact: {self:?}"
        )
    }
}

impl std::error::Error for DescribeFeaturesMachineError {}
