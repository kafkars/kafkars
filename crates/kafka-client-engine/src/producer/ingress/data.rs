//! Deterministic producer host ownership behind one shard lock.

use kafka_client_core::Moment;

use crate::{
    clock::OperationDeadline,
    producer::{
        ProducerHost, ProducerHostInvariantError, ProducerRecord,
        admission::{AdmittedExplicit, ProducerAdmissionFailure},
        execution::{PreparedProduceHandoffError, PreparedProduceSubmission},
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

pub(super) enum ProducerShardAdmission {
    Running,
    Closed,
}

/// Sole synchronized owner of immediate producer admission and host execution.
pub(crate) struct ProducerShardData {
    pub(super) host: ProducerHost,
    pub(super) admission: ProducerShardAdmission,
}

impl ProducerShardData {
    pub(super) const fn new(host: ProducerHost) -> Self {
        Self {
            host,
            admission: ProducerShardAdmission::Running,
        }
    }

    /// Closes immediate admission while the one shard mutex is held.
    pub(crate) fn close_admission(&mut self) {
        if matches!(&self.admission, ProducerShardAdmission::Running) {
            self.admission = ProducerShardAdmission::Closed;
        }
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
            accepting: matches!(&self.admission, ProducerShardAdmission::Running),
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
