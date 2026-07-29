//! Explicit all-broker lifecycle vocabulary for Admin `ListTransactions`.

use core::fmt;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    super::DescribeClusterBrokerError, AdminListTransactionsBrokerError,
    AdminListTransactionsBrokerOutcome, AdminListTransactionsPlan, AdminListTransactionsTerminal,
    AdminListedTransaction,
};

/// Current ownership stage for one cluster-wide listing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminListTransactionsState {
    /// Accepted but not started.
    Ready,
    /// Controller-routed broker discovery awaits driver admission.
    AwaitingDiscoveryDriver,
    /// The driver owns broker discovery.
    DiscoverySubmitted,
    /// One exact broker listing awaits driver admission.
    AwaitingBrokerDriver,
    /// The driver owns one exact broker listing.
    BrokerSubmitted,
    /// Core assigned the sole terminal decision.
    Completed,
}

/// One normalized fact applied to transaction-listing policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdminListTransactionsInput {
    /// Starts execution at the supplied monotonic observation.
    Start {
        /// Current monotonic observation.
        now: Moment,
    },
    /// Reports driver ownership of the current request.
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
    /// Reports discovered broker identities.
    BrokersDiscovered {
        /// Broker identities; core validates and sorts this complete set.
        broker_ids: Vec<i32>,
    },
    /// Reports an exact top-level discovery rejection.
    DiscoveryRejected {
        /// Exact code and bounded diagnostic from `DescribeCluster`.
        error: DescribeClusterBrokerError,
    },
    /// Reports one correlated exact-broker `ListTransactions` outcome.
    BrokerResponded {
        /// Nonnegative throttle observation from this broker.
        throttle_time_ms: u32,
        /// Exact correlated broker outcome.
        outcome: AdminListTransactionsBrokerOutcome,
    },
    /// Reports a structurally valid response exceeding retained capacity.
    ResponseTooLarge,
    /// Reports insufficient negotiated protocol semantics.
    ProtocolIncompatible {
        /// Driver-authoritative delivery certainty.
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

/// One concrete mechanism request emitted by deterministic policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdminListTransactionsEffect {
    /// Submit broker discovery under the original public deadline.
    SubmitDiscovery {
        /// Stable identity reserved before machine construction.
        operation_id: OperationId,
        /// Original public absolute deadline.
        deadline: Deadline,
    },
    /// Submit one filtered listing to one exact discovered broker.
    SubmitBroker {
        /// Stable identity reserved before machine construction.
        operation_id: OperationId,
        /// Original public absolute deadline.
        deadline: Deadline,
        /// Exact broker identity.
        broker_id: i32,
        /// Exact bounded filter plan.
        plan: AdminListTransactionsPlan,
    },
    /// Publish the sole terminal decision.
    Complete {
        /// Stable operation identity.
        operation_id: OperationId,
        /// Core-owned terminal decision.
        terminal: AdminListTransactionsTerminal,
    },
}

/// Ordered result of one deterministic state transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminListTransactionsTransition {
    effect: Option<AdminListTransactionsEffect>,
}

impl AdminListTransactionsTransition {
    pub(crate) const fn none() -> Self {
        Self { effect: None }
    }

    pub(crate) const fn one(effect: AdminListTransactionsEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Consumes this transition into its optional effect.
    pub fn into_effect(self) -> Option<AdminListTransactionsEffect> {
        self.effect
    }
}

/// Deterministic owner for one capacity-reserved cluster-wide listing.
#[derive(Debug)]
pub struct AdminListTransactionsMachine {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: Deadline,
    pub(crate) plan: AdminListTransactionsPlan,
    pub(crate) state: AdminListTransactionsState,
    pub(crate) broker_ids: Vec<i32>,
    pub(crate) next_broker: usize,
    pub(crate) maximum_throttle_time_ms: u32,
    pub(crate) unknown_state_filters: Vec<String>,
    pub(crate) transactions: Vec<AdminListedTransaction>,
    pub(crate) broker_errors: Vec<AdminListTransactionsBrokerError>,
    pub(crate) unknown_state_filter_count: usize,
    pub(crate) transaction_count: usize,
    pub(crate) result_string_bytes: usize,
    pub(crate) completed_calls: usize,
}

impl AdminListTransactionsMachine {
    /// Creates one accepted operation after terminal and result-byte reservation.
    pub const fn new(
        operation_id: OperationId,
        deadline: Deadline,
        plan: AdminListTransactionsPlan,
    ) -> Self {
        Self {
            operation_id,
            deadline,
            plan,
            state: AdminListTransactionsState::Ready,
            broker_ids: Vec::new(),
            next_broker: 0,
            maximum_throttle_time_ms: 0,
            unknown_state_filters: Vec::new(),
            transactions: Vec::new(),
            broker_errors: Vec::new(),
            unknown_state_filter_count: 0,
            transaction_count: 0,
            result_string_bytes: 0,
            completed_calls: 0,
        }
    }

    /// Returns the current lifecycle stage.
    pub const fn state(&self) -> AdminListTransactionsState {
        self.state
    }

    /// Returns the exact broker currently awaiting or owned by the driver.
    pub fn current_broker(&self) -> Option<i32> {
        self.broker_ids.get(self.next_broker).copied()
    }
}

/// Rejected transaction-listing state-machine fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminListTransactionsMachineError {
    /// The fact does not belong to the current lifecycle stage.
    InvalidState,
    /// The operation already owns its terminal decision.
    AlreadyCompleted,
}

impl fmt::Display for AdminListTransactionsMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "ListTransactions machine rejected fact: {self:?}"
        )
    }
}

impl std::error::Error for AdminListTransactionsMachineError {}
