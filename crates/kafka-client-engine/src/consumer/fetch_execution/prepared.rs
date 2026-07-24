//! Linear preparation of one core-authorized direct-consumer Fetch.

use kafka_client_core::{
    AssignedConsumerEffect, AssignedConsumerMachine, AssignedConsumerMachineError, FetchOwnership,
};

use crate::{
    clock::OperationDeadline,
    driver::{FetchRequestPreparationError, PartitionFetchRequest},
    protocol::fetch::{FetchDecodeLimits, FetchRequestSettings},
};

/// One exact Fetch effect paired with every fact needed before driver admission.
#[must_use = "a prepared Fetch must be submitted or explicitly abandoned"]
pub(crate) struct PreparedFetchExecution {
    pub(super) request: PartitionFetchRequest,
    pub(super) hard_output_bytes: usize,
}

impl PreparedFetchExecution {
    #[allow(
        clippy::too_many_arguments,
        reason = "preparation makes the complete execution boundary explicit"
    )]
    pub(crate) fn new(
        effect: AssignedConsumerEffect,
        topic: String,
        settings: FetchRequestSettings,
        decode_limits: FetchDecodeLimits,
        operation_deadline: OperationDeadline,
        hard_output_bytes: usize,
    ) -> Result<Self, PrepareFetchError> {
        let request = PartitionFetchRequest::from_effect(
            effect,
            topic,
            settings,
            decode_limits,
            operation_deadline,
        )
        .map_err(PrepareFetchError::from)?;
        Ok(Self {
            request,
            hard_output_bytes,
        })
    }

    pub(crate) const fn fence(&self) -> kafka_client_core::FetchFence {
        self.request.fence()
    }

    /// Reconciles queued work against the core-owned directional fence policy.
    #[allow(
        clippy::result_large_err,
        reason = "an ownership error must retain the exact linear prepared Fetch"
    )]
    pub(crate) fn reconcile_ownership(
        self,
        machine: &AssignedConsumerMachine,
    ) -> Result<Option<Self>, (AssignedConsumerMachineError, Self)> {
        match machine.fetch_ownership(self.fence()) {
            Ok(FetchOwnership::Active) => Ok(Some(self)),
            Ok(FetchOwnership::Superseded) => Ok(None),
            Err(error) => Err((error, self)),
        }
    }

    pub(super) const fn from_parts(
        request: PartitionFetchRequest,
        hard_output_bytes: usize,
    ) -> Self {
        Self {
            request,
            hard_output_bytes,
        }
    }

    #[cfg(test)]
    pub(crate) fn into_parts_for_test(self) -> (PartitionFetchRequest, usize) {
        (self.request, self.hard_output_bytes)
    }
}

/// Preparation failure before any retained-output capacity is acquired.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PrepareFetchError {
    UnexpectedEffect,
}

impl From<FetchRequestPreparationError> for PrepareFetchError {
    fn from(error: FetchRequestPreparationError) -> Self {
        match error {
            FetchRequestPreparationError::UnexpectedEffect => Self::UnexpectedEffect,
        }
    }
}
