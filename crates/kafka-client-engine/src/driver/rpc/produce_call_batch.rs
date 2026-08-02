//! Fail-closed ownership for a not-yet-authorized aggregated Produce call.

use kafka_client_core::{
    BatchExecutionId, DeliveryStatus, Moment, ProducerAttemptFailureKind, ProducerInput,
};

use crate::producer::execution::PreparedProduceSubmission;

use super::{
    super::DriverOwner, calls::ProduceCallPermit, produce_acceptance::AcceptedProduceCall,
};

/// Linear proof that one accepted request owns every named core execution.
#[must_use = "driver acceptance must be applied to every aggregated Produce execution"]
pub(crate) struct AcceptedProduceBatchCall {
    receipts: Vec<AcceptedProduceCall>,
}

/// Definitely-unsent rejection retaining every execution affected by one attempt.
pub(crate) struct ProduceBatchSubmitFailure {
    submissions: Vec<PreparedProduceSubmission>,
    kind: ProducerAttemptFailureKind,
}

impl ProduceCallPermit<'_> {
    /// Rejects aggregation until the driver can accept one opaque name-routed
    /// call without exposing or trusting a raw broker identity.
    pub(crate) fn submit_batch(
        self,
        _driver: &DriverOwner,
        submissions: Vec<PreparedProduceSubmission>,
        _now: Moment,
    ) -> Result<AcceptedProduceBatchCall, ProduceBatchSubmitFailure> {
        debug_assert!(submissions.len() > 1);
        drop(self);
        Err(ProduceBatchSubmitFailure::from_submissions(
            submissions,
            ProducerAttemptFailureKind::Permanent,
        ))
    }
}

impl AcceptedProduceBatchCall {
    pub(crate) fn inputs(&self) -> impl Iterator<Item = ProducerInput> + '_ {
        self.receipts
            .iter()
            .map(AcceptedProduceCall::driver_accepted)
    }

    pub(crate) fn confirm_receipt(self) {
        for receipt in self.receipts {
            receipt.confirm_receipt();
        }
    }
}

impl ProduceBatchSubmitFailure {
    fn from_submissions(
        submissions: Vec<PreparedProduceSubmission>,
        kind: ProducerAttemptFailureKind,
    ) -> Self {
        Self { submissions, kind }
    }

    pub(crate) fn delivery(&self) -> DeliveryStatus {
        let _retained_count = self.submissions.len();
        DeliveryStatus::NotSent
    }

    pub(crate) const fn failure_kind(&self) -> ProducerAttemptFailureKind {
        self.kind
    }

    pub(crate) fn executions(&self) -> impl Iterator<Item = BatchExecutionId> + '_ {
        self.submissions
            .iter()
            .map(PreparedProduceSubmission::execution)
    }
}
