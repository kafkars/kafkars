//! Single-owner lifecycle vocabulary for Admin `DescribeTransactions`.

use core::fmt;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    AdminDescribeTransactionOutcome, AdminDescribeTransactionsPlan,
    AdminDescribeTransactionsTerminal,
};

/// Current ownership stage for one transaction-description query.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminDescribeTransactionsState {
    /// Accepted but not yet started.
    Ready,
    /// One exact transactional ID awaits driver admission.
    AwaitingDriver,
    /// The driver owns one exact coordinator-routed call.
    Submitted,
    /// Core assigned the sole terminal decision.
    Completed,
}

/// One normalized fact applied to transaction-description policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdminDescribeTransactionsInput {
    /// Starts execution at the supplied monotonic observation.
    Start {
        /// Current monotonic observation.
        now: Moment,
    },
    /// Reports that the driver accepted the current coordinator call.
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
    /// Reports one correlated per-ID broker outcome.
    BrokerResponded {
        /// Nonnegative broker throttle observation.
        throttle_time_ms: u32,
        /// Exact result identity and value.
        outcome: AdminDescribeTransactionOutcome,
    },
    /// Reports a structurally valid response exceeding retained capacity.
    ResponseTooLarge,
    /// Reports insufficient negotiated protocol semantics.
    ProtocolIncompatible {
        /// Authoritative certainty at incompatibility discovery.
        delivery: DeliveryStatus,
    },
    /// Reports a driver-owned transport terminal.
    TransportFailed {
        /// Driver-authoritative delivery certainty.
        delivery: DeliveryStatus,
    },
    /// Reports malformed or uncorrelatable response data.
    InvalidResponse,
}

/// One concrete mechanism request emitted by core policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdminDescribeTransactionsEffect {
    /// Materialize and coordinator-route one ID under the original deadline.
    Submit {
        /// Stable identity reserved before machine construction.
        operation_id: OperationId,
        /// Original public absolute deadline.
        deadline: Deadline,
        /// Exact current transactional ID.
        transactional_id: String,
    },
    /// Publish the sole terminal decision.
    Complete {
        /// Stable operation identity.
        operation_id: OperationId,
        /// Core-owned terminal decision.
        terminal: AdminDescribeTransactionsTerminal,
    },
}

/// Ordered result of one deterministic state transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminDescribeTransactionsTransition {
    effect: Option<AdminDescribeTransactionsEffect>,
}

impl AdminDescribeTransactionsTransition {
    pub(crate) const fn none() -> Self {
        Self { effect: None }
    }

    pub(crate) const fn one(effect: AdminDescribeTransactionsEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Consumes this transition into its optional effect.
    pub fn into_effect(self) -> Option<AdminDescribeTransactionsEffect> {
        self.effect
    }
}

/// Deterministic owner for one capacity-reserved transaction query.
#[derive(Debug)]
pub struct AdminDescribeTransactionsMachine {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: Deadline,
    pub(crate) plan: AdminDescribeTransactionsPlan,
    pub(crate) state: AdminDescribeTransactionsState,
    pub(crate) next_transaction: usize,
    pub(crate) maximum_throttle_time_ms: u32,
    pub(crate) topic_count: usize,
    pub(crate) partition_count: usize,
    pub(crate) topic_bytes: usize,
    pub(crate) outcomes: Vec<AdminDescribeTransactionOutcome>,
}

impl AdminDescribeTransactionsMachine {
    /// Creates one accepted operation after engine terminal and byte reservation.
    pub fn new(
        operation_id: OperationId,
        deadline: Deadline,
        plan: AdminDescribeTransactionsPlan,
    ) -> Self {
        let outcomes = Vec::with_capacity(plan.transactional_ids().len());
        Self {
            operation_id,
            deadline,
            plan,
            state: AdminDescribeTransactionsState::Ready,
            next_transaction: 0,
            maximum_throttle_time_ms: 0,
            topic_count: 0,
            partition_count: 0,
            topic_bytes: 0,
            outcomes,
        }
    }

    /// Returns the current lifecycle stage.
    pub const fn state(&self) -> AdminDescribeTransactionsState {
        self.state
    }

    /// Returns the exact ID currently awaiting or owned by the driver.
    pub fn current_transactional_id(&self) -> Option<&str> {
        self.plan
            .transactional_ids()
            .get(self.next_transaction)
            .map(String::as_str)
    }
}

/// Rejected transaction-description state-machine fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminDescribeTransactionsMachineError {
    /// The fact does not belong to the current lifecycle stage.
    InvalidState,
    /// The operation already owns its terminal decision.
    AlreadyCompleted,
}

impl fmt::Display for AdminDescribeTransactionsMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "DescribeTransactions machine rejected fact: {self:?}"
        )
    }
}

impl std::error::Error for AdminDescribeTransactionsMachineError {}
