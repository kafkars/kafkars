//! Fatal invariant observations with every linear owner retained for shutdown.

use kafka_client_core::{AssignedConsumerMachineError, AssignedConsumerTransition, FetchFence};

use crate::{
    driver::{
        FetchBeginSettlementError, FetchCompletionObservation, FetchConfirmationError,
        FetchControlPending, FetchRecovery, PartitionFetchRequest, StaleFetchConfirmationError,
    },
    protocol::fetch::{FetchOutputReservation, RetainedFetchOutcome},
};

use super::{
    super::{
        assigned_event::AssignedConsumerEventStoreError,
        fetch_store::{FetchDelivery, FetchStageProof, FetchStoreFailure},
    },
    prepared::PreparedFetchExecution,
};

/// Stable fatal observation; detailed linear ownership stays inside the executor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FetchExecutionError {
    Faulted,
    BrokerRouteCompletion,
    BrokerSession,
    Completion(FetchCompletionObservation),
    ControlPending(FetchControlPending),
    Begin(FetchBeginSettlementError),
    Confirm(FetchConfirmationError),
    ConfirmStale(StaleFetchConfirmationError),
    MissingReservation { fence: FetchFence },
    UnexpectedStaleReservation { fence: FetchFence },
    Store(FetchStoreFailure),
    Event(AssignedConsumerEventStoreError),
    Core(AssignedConsumerMachineError),
    UnexpectedDeliveryAuthorization { fence: FetchFence },
    UnexpectedRetryAuthorization { fence: FetchFence },
    UnexpectedRetryStorage { fence: FetchFence },
}

#[allow(
    clippy::large_enum_variant,
    reason = "fatal ownership remains allocation-free and is released after driver shutdown"
)]
pub(super) enum RetainedFetchFault {
    Prepared {
        _prepared: PreparedFetchExecution,
    },
    PreparedRollback {
        _prepared: PreparedFetchExecution,
        _proof: FetchStageProof,
        _output: FetchOutputReservation,
    },
    ControlRollback {
        _requests: Vec<PartitionFetchRequest>,
        _proof: FetchStageProof,
        _output: FetchOutputReservation,
    },
    ControlRequests {
        _requests: Vec<PartitionFetchRequest>,
    },
    Outcome {
        _request: PartitionFetchRequest,
        _proof: FetchStageProof,
        _outcome: RetainedFetchOutcome,
    },
    Request {
        _request: PartitionFetchRequest,
    },
    Registry,
    Staged,
    Transition {
        _request: PartitionFetchRequest,
        _transition: AssignedConsumerTransition,
    },
}

/// Failed lease reclamation returning exact application ownership.
#[must_use = "a failed Fetch reclaim still owns the delivery lease"]
pub(crate) struct FetchReclaimFailure {
    error: FetchExecutionError,
    delivery: FetchDelivery,
}

impl FetchReclaimFailure {
    pub(super) const fn new(error: FetchExecutionError, delivery: FetchDelivery) -> Self {
        Self { error, delivery }
    }

    pub(crate) fn into_parts(self) -> (FetchExecutionError, FetchDelivery) {
        (self.error, self.delivery)
    }
}

/// Explicit ownership released only after the embedded driver has shut down.
#[must_use = "post-driver-shutdown Fetch recovery observations must be handled"]
pub(crate) struct FetchShutdownRecovery {
    driver: FetchRecovery,
    had_fault: bool,
}

impl FetchShutdownRecovery {
    pub(crate) fn into_driver_recovery(self) -> FetchRecovery {
        self.driver
    }

    pub(crate) const fn had_fault(&self) -> bool {
        self.had_fault
    }

    pub(super) const fn new(driver: FetchRecovery, had_fault: bool) -> Self {
        Self { driver, had_fault }
    }
}
