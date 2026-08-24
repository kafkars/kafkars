//! Linear preparation of one core-authorized direct-consumer Fetch.

use kafka_client_core::{
    AssignedConsumerEffect, AssignedConsumerMachine, AssignedConsumerMachineError, Deadline,
    FetchOwnership,
};

use crate::{
    driver::PartitionFetchRequest,
    protocol::fetch::{FetchDecodeLimits, FetchRequestSettings},
};

use super::FetchAttemptDeadline;

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
        attempt_deadline: FetchAttemptDeadline,
        hard_output_bytes: usize,
    ) -> Result<Self, PrepareFetchError> {
        Self::prepare(
            effect,
            topic,
            settings,
            decode_limits,
            attempt_deadline,
            hard_output_bytes,
        )
        .map_err(PrepareFetchFailure::into_error)
    }

    #[allow(
        clippy::too_many_arguments,
        clippy::result_large_err,
        reason = "lossless preparation retains the captured attempt at the complete boundary"
    )]
    pub(crate) fn new_retaining_attempt(
        effect: AssignedConsumerEffect,
        topic: String,
        settings: FetchRequestSettings,
        decode_limits: FetchDecodeLimits,
        attempt_deadline: FetchAttemptDeadline,
        hard_output_bytes: usize,
    ) -> Result<Self, PrepareFetchFailure> {
        Self::prepare(
            effect,
            topic,
            settings,
            decode_limits,
            attempt_deadline,
            hard_output_bytes,
        )
    }

    #[allow(
        clippy::too_many_arguments,
        clippy::result_large_err,
        reason = "lossless preparation retains the captured attempt at the complete boundary"
    )]
    fn prepare(
        effect: AssignedConsumerEffect,
        topic: String,
        settings: FetchRequestSettings,
        decode_limits: FetchDecodeLimits,
        attempt_deadline: FetchAttemptDeadline,
        hard_output_bytes: usize,
    ) -> Result<Self, PrepareFetchFailure> {
        let AssignedConsumerEffect::FetchReady { fence, next_offset } = effect else {
            return Err(PrepareFetchFailure::new(
                PrepareFetchError::UnexpectedEffect,
                attempt_deadline,
            ));
        };
        if attempt_deadline.fence() != fence {
            return Err(PrepareFetchFailure::new(
                PrepareFetchError::DeadlineFenceMismatch {
                    effect: fence,
                    captured: attempt_deadline.fence(),
                },
                attempt_deadline,
            ));
        }
        let request = PartitionFetchRequest::from_fetch_ready_parts(
            fence,
            next_offset,
            topic,
            settings,
            decode_limits,
            attempt_deadline.into_operation(),
        );
        Ok(Self {
            request,
            hard_output_bytes,
        })
    }

    pub(crate) const fn fence(&self) -> kafka_client_core::FetchFence {
        self.request.fence()
    }

    /// Borrows the original core deadline without exposing transport timing.
    pub(crate) const fn deadline(&self) -> Deadline {
        self.request.operation_deadline().core()
    }

    pub(super) fn is_superseded_by(&self, effect: AssignedConsumerEffect) -> bool {
        self.request.is_superseded_by(effect)
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

    pub(super) fn into_parts(self) -> (PartitionFetchRequest, usize) {
        (self.request, self.hard_output_bytes)
    }

    #[cfg(test)]
    pub(crate) fn into_parts_for_test(self) -> (PartitionFetchRequest, usize) {
        (self.request, self.hard_output_bytes)
    }
}

/// Failed preparation retaining the exact freshly captured attempt boundary.
#[must_use = "a failed Fetch preparation still owns its captured attempt deadline"]
pub(crate) struct PrepareFetchFailure {
    error: PrepareFetchError,
    attempt: FetchAttemptDeadline,
}

impl PrepareFetchFailure {
    fn new(error: PrepareFetchError, attempt: FetchAttemptDeadline) -> Self {
        Self { error, attempt }
    }

    pub(crate) const fn error(&self) -> PrepareFetchError {
        self.error
    }

    pub(crate) const fn attempt(&self) -> &FetchAttemptDeadline {
        &self.attempt
    }

    pub(crate) fn into_parts(self) -> (PrepareFetchError, FetchAttemptDeadline) {
        (self.error, self.attempt)
    }

    fn into_error(self) -> PrepareFetchError {
        self.error
    }
}

/// Preparation failure before any retained-output capacity is acquired.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PrepareFetchError {
    UnexpectedEffect,
    DeadlineFenceMismatch {
        effect: kafka_client_core::FetchFence,
        captured: kafka_client_core::FetchFence,
    },
}
