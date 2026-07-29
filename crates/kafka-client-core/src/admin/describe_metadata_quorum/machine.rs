//! Single-owner lifecycle vocabulary for one fixed metadata-quorum query.

use core::fmt;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    DescribeMetadataQuorumBrokerError, DescribeMetadataQuorumDescription,
    DescribeMetadataQuorumPartitionError, DescribeMetadataQuorumTerminal,
};

/// Current ownership stage for one metadata-quorum description.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeMetadataQuorumState {
    /// Accepted but not started.
    Ready,
    /// The sole fixed request awaits driver admission.
    AwaitingDriver,
    /// The driver owns the sole request attempt.
    Submitted,
    /// Core assigned the sole terminal decision.
    Completed,
}

/// One normalized fact applied to metadata-quorum description policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DescribeMetadataQuorumInput {
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
    /// Reports one bounded protocol-normalized fixed-quorum description.
    BrokerResponded {
        /// Wire-free fixed metadata-quorum facts.
        description: DescribeMetadataQuorumDescription,
    },
    /// Reports Kafka's exact top-level rejection.
    BrokerRejected {
        /// Exact signed code and bounded nullable diagnostic.
        error: DescribeMetadataQuorumBrokerError,
    },
    /// Reports Kafka's exact fixed-partition rejection.
    PartitionRejected {
        /// Exact signed code and bounded nullable diagnostic.
        error: DescribeMetadataQuorumPartitionError,
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

/// One concrete mechanism request emitted by metadata-quorum policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DescribeMetadataQuorumEffect {
    /// Submit the fixed metadata-quorum request exactly once.
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
        terminal: DescribeMetadataQuorumTerminal,
    },
}

/// Ordered result of one deterministic state transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeMetadataQuorumTransition {
    effect: Option<DescribeMetadataQuorumEffect>,
}

impl DescribeMetadataQuorumTransition {
    pub(crate) const fn none() -> Self {
        Self { effect: None }
    }

    pub(crate) const fn one(effect: DescribeMetadataQuorumEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Consumes this transition into its optional concrete effect.
    pub fn into_effect(self) -> Option<DescribeMetadataQuorumEffect> {
        self.effect
    }
}

/// Deterministic owner for one capacity-reserved metadata-quorum query.
#[derive(Debug)]
pub struct DescribeMetadataQuorumMachine {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: Deadline,
    pub(crate) state: DescribeMetadataQuorumState,
}

impl DescribeMetadataQuorumMachine {
    /// Creates one accepted query after engine terminal and byte reservation.
    pub const fn new(operation_id: OperationId, deadline: Deadline) -> Self {
        Self {
            operation_id,
            deadline,
            state: DescribeMetadataQuorumState::Ready,
        }
    }

    /// Returns the current lifecycle stage.
    pub const fn state(&self) -> DescribeMetadataQuorumState {
        self.state
    }
}

/// Rejected deterministic metadata-quorum state-machine fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeMetadataQuorumMachineError {
    /// The fact does not belong to the current lifecycle stage.
    InvalidState,
    /// The operation already owns its terminal decision.
    AlreadyCompleted,
}

impl fmt::Display for DescribeMetadataQuorumMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "DescribeMetadataQuorum machine rejected fact: {self:?}"
        )
    }
}

impl std::error::Error for DescribeMetadataQuorumMachineError {}
