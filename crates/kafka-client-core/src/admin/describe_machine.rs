//! Single-owner lifecycle vocabulary for one `DescribeCluster` operation.

use core::fmt;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{ClusterDescription, DescribeClusterBrokerError, DescribeClusterTerminal};

/// Current ownership stage for one `DescribeCluster` operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DescribeClusterState {
    /// Accepted but not yet offered to the driver.
    Ready,
    /// The operation awaits driver admission.
    AwaitingDriver,
    /// The driver owns the request.
    Submitted,
    /// Core assigned the sole terminal outcome.
    Completed,
}

/// One normalized fact applied to `DescribeCluster` policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DescribeClusterInput {
    /// Starts execution at the supplied monotonic observation.
    Start {
        /// Current monotonic observation.
        now: Moment,
    },
    /// Reports that the driver accepted request ownership.
    DriverAccepted,
    /// Reports definite rejection before driver ownership.
    DriverRejected,
    /// Reports original-deadline expiry before driver ownership.
    DeadlineElapsed,
    /// Reports one normalized successful broker response.
    BrokerResponded {
        /// Bounded cluster facts.
        description: ClusterDescription,
    },
    /// Reports an exact top-level broker rejection.
    BrokerRejected {
        /// Exact broker code and bounded nullable diagnostic.
        error: DescribeClusterBrokerError,
    },
    /// Reports a driver-owned transport terminal.
    TransportFailed {
        /// Driver-authoritative delivery certainty.
        delivery: DeliveryStatus,
    },
    /// Reports that the broker cannot represent the explicitly requested view.
    ProtocolIncompatible {
        /// Driver-authoritative delivery certainty.
        delivery: DeliveryStatus,
    },
    /// Reports that SASL authentication failed before ordinary call admission.
    AuthenticationFailed {
        /// Driver-authoritative delivery certainty.
        delivery: DeliveryStatus,
    },
    /// Reports a malformed or over-budget broker response.
    InvalidResponse,
}

/// One concrete mechanism request emitted by `DescribeCluster` policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DescribeClusterEffect {
    /// Submit the broker-endpoint request options with its original deadline.
    Submit {
        /// Stable identity reserved before machine construction.
        operation_id: OperationId,
        /// Original public absolute deadline.
        deadline: Deadline,
        /// Whether the caller explicitly requested fenced brokers.
        include_fenced_brokers: bool,
        /// Whether the caller explicitly requested cluster authorization bits.
        include_authorized_operations: bool,
    },
    /// Publish the one terminal decision.
    Complete {
        /// Stable operation identity.
        operation_id: OperationId,
        /// Core-owned terminal decision.
        terminal: DescribeClusterTerminal,
    },
}

/// Ordered result of one `DescribeCluster` state transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeClusterTransition {
    effect: Option<DescribeClusterEffect>,
}

impl DescribeClusterTransition {
    pub(crate) const fn none() -> Self {
        Self { effect: None }
    }

    pub(crate) const fn one(effect: DescribeClusterEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Consumes the transition into its optional effect.
    pub fn into_effect(self) -> Option<DescribeClusterEffect> {
        self.effect
    }
}

/// Deterministic owner for one capacity-reserved `DescribeCluster` operation.
#[derive(Debug)]
pub struct DescribeClusterMachine {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: Deadline,
    pub(crate) include_fenced_brokers: bool,
    pub(crate) include_authorized_operations: bool,
    pub(crate) state: DescribeClusterState,
}

impl DescribeClusterMachine {
    /// Creates an accepted operation after engine terminal reservation.
    pub const fn new(operation_id: OperationId, deadline: Deadline) -> Self {
        Self::new_with_options(operation_id, deadline, false, false)
    }

    /// Creates an accepted operation with its explicit broker-view policy.
    pub const fn new_with_fenced_brokers(
        operation_id: OperationId,
        deadline: Deadline,
        include_fenced_brokers: bool,
    ) -> Self {
        Self::new_with_options(operation_id, deadline, include_fenced_brokers, false)
    }

    /// Creates an accepted operation with both explicit cluster-view policies.
    pub const fn new_with_options(
        operation_id: OperationId,
        deadline: Deadline,
        include_fenced_brokers: bool,
        include_authorized_operations: bool,
    ) -> Self {
        Self {
            operation_id,
            deadline,
            include_fenced_brokers,
            include_authorized_operations,
            state: DescribeClusterState::Ready,
        }
    }

    /// Returns the current lifecycle stage.
    pub const fn state(&self) -> DescribeClusterState {
        self.state
    }
}

/// Rejected `DescribeCluster` state-machine fact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DescribeClusterMachineError {
    /// The fact does not belong to the current lifecycle stage.
    InvalidState,
    /// The operation already owns its terminal decision.
    AlreadyCompleted,
}

impl fmt::Display for DescribeClusterMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "DescribeCluster machine rejected fact: {self:?}")
    }
}

impl std::error::Error for DescribeClusterMachineError {}
