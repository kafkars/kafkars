//! Exact-broker handoff for single and aggregated Produce calls.

#[cfg(test)]
use kafka_client_core::DeliveryStatus;
use kafka_client_core::{BatchExecutionId, Moment, ProducerAttemptFailureKind, ProducerInput};

use crate::{
    clock::OperationDeadline, producer::execution::PreparedProduceSubmission,
    protocol::produce::MaterializedProduce,
};

use super::{
    super::DriverOwner,
    ProduceSubmitError,
    calls::ProduceCallPermit,
    produce_acceptance::AcceptedProduceCall,
    produce_call_entries::{TrackedProduceEntries, TrackedProduceEntry},
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
    /// Submits one generated request to the broker carried by this permit.
    pub(crate) fn submit(
        self,
        driver: &DriverOwner,
        execution: BatchExecutionId,
        deadline: OperationDeadline,
        materialized: MaterializedProduce,
        now: Moment,
    ) -> Result<AcceptedProduceCall, ProduceSubmitError> {
        let topic = materialized.topic_owner();
        let partition = materialized.partition();
        let request = materialized.into_name_routed_request(now, deadline);
        let broker_id = self.reserved_exact_broker_id();
        let call =
            driver.submit_tracked_produce_to_broker(broker_id, request, deadline.transport())?;
        self.commit_reserved_exact_broker_call(
            TrackedProduceEntries::Single(TrackedProduceEntry {
                execution,
                deadline: deadline.core(),
                topic,
                partition,
            }),
            deadline,
            call,
        );
        Ok(AcceptedProduceCall::new(execution))
    }

    /// Submits every retained partition batch through one exact broker call.
    pub(crate) fn submit_batch(
        self,
        driver: &DriverOwner,
        submissions: Vec<PreparedProduceSubmission>,
        now: Moment,
    ) -> Result<AcceptedProduceBatchCall, ProduceBatchSubmitFailure> {
        let Some(deadline) = shared_deadline(&submissions) else {
            return Err(ProduceBatchSubmitFailure::from_submissions(
                submissions,
                ProducerAttemptFailureKind::Permanent,
            ));
        };
        let retained = submissions.len();
        let mut batches = Vec::new();
        let mut entries = Vec::new();
        let mut receipts = Vec::new();
        if batches.try_reserve_exact(retained).is_err()
            || entries.try_reserve_exact(retained).is_err()
            || receipts.try_reserve_exact(retained).is_err()
        {
            return Err(ProduceBatchSubmitFailure::from_submissions(
                submissions,
                ProducerAttemptFailureKind::LocalCapacity,
            ));
        }
        for submission in &submissions {
            let materialized = submission.materialized();
            batches.push(materialized);
            entries.push(TrackedProduceEntry {
                execution: submission.execution(),
                deadline: deadline.core(),
                topic: materialized.topic_owner(),
                partition: materialized.partition(),
            });
            receipts.push(AcceptedProduceCall::new(submission.execution()));
        }
        let Some(request) = MaterializedProduce::broker_routed_request(&batches, now, deadline)
        else {
            drop(batches);
            return Err(ProduceBatchSubmitFailure::from_submissions(
                submissions,
                ProducerAttemptFailureKind::LocalCapacity,
            ));
        };
        drop(batches);
        let broker_id = self.reserved_exact_broker_id();
        let call =
            match driver.submit_tracked_produce_to_broker(broker_id, request, deadline.transport())
            {
                Ok(call) => call,
                Err(error) => {
                    return Err(ProduceBatchSubmitFailure::from_submissions(
                        submissions,
                        error.failure_kind(),
                    ));
                }
            };
        drop(submissions);
        self.commit_reserved_exact_broker_call(
            TrackedProduceEntries::batch(entries),
            deadline,
            call,
        );
        Ok(AcceptedProduceBatchCall { receipts })
    }
}

fn shared_deadline(submissions: &[PreparedProduceSubmission]) -> Option<OperationDeadline> {
    let deadline = submissions.first()?.deadline();
    (submissions.len() > 1 && submissions.iter().all(|entry| entry.deadline() == deadline))
        .then_some(deadline)
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

    #[cfg(test)]
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
