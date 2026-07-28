//! Deterministic producer host ownership behind one shard lock.

use kafka_client_core::{Moment, partitioning::TopicPartitionSource};

use crate::{
    clock::OperationDeadline,
    producer::{
        ProducerHost, ProducerHostInvariantError, ProducerIdentityHandoffError,
        ProducerIdentitySubmission, ProducerPartitioningFailure, ProducerPartitioningRequest,
        ProducerRecord,
        admission::{AdmittedExplicit, ProducerAdmissionFailure},
        cancellation::{ProducerHostCancelAccepted, ProducerHostCancelError},
        execution::{PreparedProduceHandoffError, PreparedProduceSubmission},
        flush::{AdmittedFlush, FlushAdmissionFailure, FlushRejectionReason},
        host::ProducerHostStats,
        host_turn::{ProducerTurnBudget, ProducerTurnOutcome},
    },
};

/// Host accounting visible without splitting the shard's synchronization owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ProducerShardStats {
    pub(crate) host: ProducerHostStats,
    pub(crate) accepting: bool,
}

/// Sole synchronized owner of immediate producer admission and host execution.
pub(crate) struct ProducerShardData {
    pub(super) host: ProducerHost,
}

impl ProducerShardData {
    pub(super) const fn new(host: ProducerHost) -> Self {
        Self { host }
    }

    /// Closes immediate admission while the one shard mutex is held.
    pub(crate) fn close_admission(&mut self) {
        self.host.close_admission();
    }

    #[allow(
        clippy::result_large_err,
        reason = "ownership-preserving rejection returns the intact record"
    )]
    pub(super) fn try_admit_explicit(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
        record: ProducerRecord,
    ) -> Result<AdmittedExplicit, ProducerAdmissionFailure> {
        self.host.try_admit_explicit(now, deadline, record)
    }

    pub(super) fn try_cancel(
        &mut self,
        operation_id: kafka_client_core::OperationId,
    ) -> Result<ProducerHostCancelAccepted, ProducerHostCancelError> {
        self.host.try_cancel_operation(operation_id)
    }

    pub(super) fn try_cancel_waiter(
        &mut self,
        waiter_id: kafka_client_core::ProducerWaiterId,
        token: &std::sync::Arc<crate::producer::waiting::WaitingToken>,
    ) -> Result<kafka_client_core::ProducerCancellationOutcome, ProducerHostCancelError> {
        self.host.try_cancel_waiter(waiter_id, token)
    }

    pub(super) fn try_admit_flush(
        &mut self,
        now: Moment,
    ) -> Result<AdmittedFlush, FlushAdmissionFailure> {
        if !self.host.admission_is_open() {
            return Err(FlushAdmissionFailure::Rejected(
                FlushRejectionReason::Closed,
            ));
        }
        self.host.try_admit_flush(now)
    }

    pub(super) fn try_admit_close(
        &mut self,
        now: Moment,
    ) -> Result<AdmittedFlush, FlushAdmissionFailure> {
        if !self.host.admission_is_open() {
            return Err(FlushAdmissionFailure::Rejected(
                FlushRejectionReason::Closed,
            ));
        }
        self.host.try_admit_close(now)
    }

    pub(crate) fn turn(
        &mut self,
        now: Moment,
        budget: ProducerTurnBudget,
    ) -> Result<ProducerTurnOutcome, ProducerHostInvariantError> {
        self.host.turn(now, budget)
    }

    /// Transfers at most one driver-ready request while this shard is locked.
    pub(crate) fn take_produce_submission(
        &mut self,
    ) -> Result<Option<PreparedProduceSubmission>, PreparedProduceHandoffError> {
        self.host.execution.take_next_driver_submission()
    }

    pub(crate) fn take_identity_submission(
        &mut self,
    ) -> Result<Option<ProducerIdentitySubmission>, ProducerIdentityHandoffError> {
        self.host.take_identity_submission()
    }

    pub(crate) fn take_partitioning_request(
        &mut self,
    ) -> Result<Option<ProducerPartitioningRequest>, ProducerHostInvariantError> {
        self.host.take_partitioning_request()
    }

    pub(crate) fn restore_partitioning_request(
        &mut self,
        request: ProducerPartitioningRequest,
    ) -> Result<(), ProducerHostInvariantError> {
        self.host.restore_partitioning_request(request)
    }

    pub(crate) fn apply_partitioning_view(
        &mut self,
        request: ProducerPartitioningRequest,
        source: &dyn TopicPartitionSource,
    ) -> Result<bool, ProducerHostInvariantError> {
        self.host.apply_partitioning_view(request, source)
    }

    pub(crate) fn apply_partitioning_failure(
        &mut self,
        request: ProducerPartitioningRequest,
        failure: ProducerPartitioningFailure,
    ) -> Result<bool, ProducerHostInvariantError> {
        self.host.apply_partitioning_failure(request, failure)
    }

    /// Applies one transport-owned fact while this shard is locked.
    pub(crate) fn apply_produce_driver_input(
        &mut self,
        now: Moment,
        input: kafka_client_core::ProducerInput,
    ) -> Result<(), ProducerHostInvariantError> {
        self.host.apply_one_driver_input(now, input)
    }

    pub(crate) fn unsettled_completions(&self) -> usize {
        self.host.unsettled_completions()
    }

    pub(crate) fn shard_stats(&self) -> ProducerShardStats {
        ProducerShardStats {
            host: self.host.stats(),
            accepting: self.host.admission_is_open(),
        }
    }

    #[cfg(test)]
    pub(crate) fn inject_post_acceptance_fault(&mut self, error: ProducerHostInvariantError) {
        self.host.inject_post_acceptance_fault(error);
    }

    #[cfg(test)]
    pub(crate) fn inject_terminal_interpretation_fault(&mut self) {
        self.host.inject_terminal_interpretation_fault();
    }

    #[cfg(test)]
    pub(crate) fn bound_deadline(
        &self,
        operation_id: kafka_client_core::OperationId,
    ) -> Option<OperationDeadline> {
        self.host.bindings.deadline(operation_id)
    }
}
