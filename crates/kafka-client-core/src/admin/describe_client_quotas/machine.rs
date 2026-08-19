//! Single-owner lifecycle vocabulary for one Admin `DescribeClientQuotas` query.

use core::fmt;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    DescribeClientQuotasBatch, DescribeClientQuotasBrokerError, DescribeClientQuotasPlan,
    DescribeClientQuotasTerminal,
};

/// Current ownership stage for one client-quota description query.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeClientQuotasState {
    /// Accepted but not started.
    Ready,
    /// The exact filter plan awaits driver admission.
    AwaitingDriver,
    /// The driver owns the sole request attempt.
    Submitted,
    /// Core assigned the sole terminal decision.
    Completed,
}

/// One normalized fact applied to deterministic client-quota description policy.
#[derive(Clone, Debug, PartialEq)]
pub enum DescribeClientQuotasInput {
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
    /// Reports one bounded protocol-normalized entity set.
    BrokerResponded {
        /// Throttle and entity facts for the successful response.
        batch: DescribeClientQuotasBatch,
    },
    /// Reports Kafka's exact top-level error and diagnostic.
    BrokerRejected {
        /// Exact signed code and bounded nullable diagnostic.
        error: DescribeClientQuotasBrokerError,
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

/// One concrete mechanism request emitted by client-quota description policy.
#[derive(Clone, Debug, PartialEq)]
pub enum DescribeClientQuotasEffect {
    /// Submit the exact filter once through the engine's `AnyBroker` lane.
    Submit {
        /// Stable identity reserved before machine construction.
        operation_id: OperationId,
        /// Original public absolute deadline.
        deadline: Deadline,
        /// Validated wire-free filter intent.
        plan: DescribeClientQuotasPlan,
    },
    /// Publish the sole terminal decision.
    Complete {
        /// Stable operation identity.
        operation_id: OperationId,
        /// Core-owned terminal decision.
        terminal: DescribeClientQuotasTerminal,
    },
}

/// Ordered result of one deterministic state transition.
#[derive(Clone, Debug, PartialEq)]
pub struct DescribeClientQuotasTransition {
    effect: Option<DescribeClientQuotasEffect>,
}

impl DescribeClientQuotasTransition {
    pub(crate) const fn none() -> Self {
        Self { effect: None }
    }

    pub(crate) const fn one(effect: DescribeClientQuotasEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Consumes this transition into its optional concrete effect.
    pub fn into_effect(self) -> Option<DescribeClientQuotasEffect> {
        self.effect
    }
}

/// Deterministic owner for one capacity-reserved client-quota query.
#[derive(Debug)]
pub struct DescribeClientQuotasMachine {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: Deadline,
    pub(crate) plan: DescribeClientQuotasPlan,
    pub(crate) state: DescribeClientQuotasState,
}

impl DescribeClientQuotasMachine {
    /// Creates one accepted query after engine terminal and byte reservation.
    pub const fn new(
        operation_id: OperationId,
        deadline: Deadline,
        plan: DescribeClientQuotasPlan,
    ) -> Self {
        Self {
            operation_id,
            deadline,
            plan,
            state: DescribeClientQuotasState::Ready,
        }
    }

    /// Returns the current lifecycle stage.
    pub const fn state(&self) -> DescribeClientQuotasState {
        self.state
    }
}

/// Rejected deterministic client-quota description state-machine fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeClientQuotasMachineError {
    /// The fact does not belong to the current lifecycle stage.
    InvalidState,
    /// The operation already owns its terminal decision.
    AlreadyCompleted,
}

impl fmt::Display for DescribeClientQuotasMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "DescribeClientQuotas machine rejected fact: {self:?}"
        )
    }
}

impl std::error::Error for DescribeClientQuotasMachineError {}
