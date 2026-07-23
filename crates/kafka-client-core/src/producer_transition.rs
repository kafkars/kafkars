//! Explicit input-to-effect transitions for the first native producer slice.

use crate::{
    AcknowledgementPolicy, BatchId, CompressionPolicy, DeliveryStatus, ExplicitRecord, OperationId,
    ProducerCompletion, ProducerEffect, ProducerInput, ProducerMachine, ProducerMachineError,
    ProducerOperationState, ProducerTransition, TransitionError,
};

impl ProducerMachine {
    /// Applies one producer fact and returns ordered engine effects.
    pub fn apply(
        &mut self,
        input: ProducerInput,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        match input {
            ProducerInput::AdmitExplicit {
                now,
                deadline,
                record,
            } => {
                let operation_id = self
                    .admit_explicit(now, deadline, record)
                    .map_err(ProducerMachineError::Admission)?;
                Ok(ProducerTransition::One([
                    ProducerEffect::AccumulateExplicit {
                        operation_id,
                        deadline,
                        record,
                    },
                ]))
            }
            ProducerInput::BatchReady {
                operation_id,
                batch_id,
                now,
            } => self.batch_ready(operation_id, batch_id, now),
            ProducerInput::DriverAccepted {
                operation_id,
                batch_id,
            } => {
                self.mark_submitted(operation_id, batch_id)?;
                Ok(ProducerTransition::None)
            }
            ProducerInput::DriverRejected {
                operation_id,
                batch_id,
            } => {
                self.require_awaiting_driver(operation_id, batch_id)?;
                self.complete_failed_transition(operation_id, DeliveryStatus::NotSent)
            }
            ProducerInput::BrokerSucceeded {
                operation_id,
                batch_id,
            } => {
                self.require_submitted(operation_id, batch_id)?;
                self.complete_delivered_transition(operation_id)
            }
            ProducerInput::BrokerFailed {
                operation_id,
                batch_id,
                delivery,
            } => {
                self.require_submitted(operation_id, batch_id)?;
                self.complete_failed_transition(operation_id, delivery)
            }
            ProducerInput::DeadlineElapsed { operation_id, now } => {
                let operation = self.operation(operation_id)?;
                let Some(deadline) = operation.deadline() else {
                    return Err(ProducerMachineError::Transition(
                        TransitionError::AlreadyCompleted,
                    ));
                };
                if !deadline.is_elapsed_at(now) {
                    return Err(ProducerMachineError::Transition(
                        TransitionError::DeadlineNotElapsed,
                    ));
                }
                let record = self.record(operation_id)?;
                let batch_id = operation.batch_id();
                self.expire_before_submission(operation_id)?;
                Ok(release_transition(
                    operation_id,
                    batch_id,
                    record,
                    ProducerCompletion::Failed(DeliveryStatus::NotSent),
                ))
            }
            ProducerInput::CompletionReclaimed { operation_id } => {
                self.reclaim_completion(operation_id)?;
                Ok(ProducerTransition::None)
            }
        }
    }

    fn batch_ready(
        &mut self,
        operation_id: OperationId,
        batch_id: BatchId,
        now: crate::Moment,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        let record = self.record(operation_id)?;
        let operation = self.operation(operation_id)?;
        let Some(deadline) = operation.deadline() else {
            return Err(ProducerMachineError::Transition(
                TransitionError::AlreadyCompleted,
            ));
        };
        if deadline.is_elapsed_at(now) {
            self.expire_before_submission(operation_id)?;
            return Ok(release_transition(
                operation_id,
                Some(batch_id),
                record,
                ProducerCompletion::Failed(DeliveryStatus::NotSent),
            ));
        }
        self.mark_ready(operation_id, batch_id)?;
        Ok(ProducerTransition::One([ProducerEffect::SubmitProduce {
            operation_id,
            batch_id,
            deadline,
            topic_id: record.topic_id(),
            partition: record.partition(),
            acknowledgements: AcknowledgementPolicy::All,
            compression: CompressionPolicy::Uncompressed,
        }]))
    }

    fn complete_delivered_transition(
        &mut self,
        operation_id: OperationId,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        let record = self.record(operation_id)?;
        let batch_id = self.operation(operation_id)?.batch_id();
        self.settle_delivered(operation_id)?;
        Ok(release_transition(
            operation_id,
            batch_id,
            record,
            ProducerCompletion::Delivered,
        ))
    }

    fn complete_failed_transition(
        &mut self,
        operation_id: OperationId,
        delivery: DeliveryStatus,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        let record = self.record(operation_id)?;
        let batch_id = self.operation(operation_id)?.batch_id();
        self.settle_failed(operation_id, delivery)?;
        Ok(release_transition(
            operation_id,
            batch_id,
            record,
            ProducerCompletion::Failed(delivery),
        ))
    }

    fn require_awaiting_driver(
        &self,
        operation_id: OperationId,
        batch_id: BatchId,
    ) -> Result<(), ProducerMachineError> {
        match self.operation(operation_id)?.state() {
            ProducerOperationState::AwaitingDriver {
                batch_id: expected, ..
            } if expected == batch_id => Ok(()),
            ProducerOperationState::AwaitingDriver { .. } => Err(ProducerMachineError::Transition(
                TransitionError::BatchMismatch,
            )),
            ProducerOperationState::Completed => Err(ProducerMachineError::Transition(
                TransitionError::AlreadyCompleted,
            )),
            ProducerOperationState::WaitingForCapacity { .. }
            | ProducerOperationState::Accumulating { .. }
            | ProducerOperationState::Submitted { .. } => Err(ProducerMachineError::Transition(
                TransitionError::InvalidState,
            )),
        }
    }

    fn require_submitted(
        &self,
        operation_id: OperationId,
        batch_id: BatchId,
    ) -> Result<(), ProducerMachineError> {
        match self.operation(operation_id)?.state() {
            ProducerOperationState::Submitted {
                batch_id: expected, ..
            } if expected == batch_id => Ok(()),
            ProducerOperationState::Submitted { .. } => Err(ProducerMachineError::Transition(
                TransitionError::BatchMismatch,
            )),
            ProducerOperationState::Completed => Err(ProducerMachineError::Transition(
                TransitionError::AlreadyCompleted,
            )),
            ProducerOperationState::WaitingForCapacity { .. }
            | ProducerOperationState::Accumulating { .. }
            | ProducerOperationState::AwaitingDriver { .. } => Err(
                ProducerMachineError::Transition(TransitionError::InvalidState),
            ),
        }
    }
}

fn release_transition(
    operation_id: OperationId,
    batch_id: Option<BatchId>,
    record: ExplicitRecord,
    completion: ProducerCompletion,
) -> ProducerTransition {
    let payload = ProducerEffect::ReleasePayload {
        payload_id: record.payload_id(),
        retained_bytes: record.retained_bytes(),
    };
    let complete = ProducerEffect::Complete {
        operation_id,
        completion,
    };
    match batch_id {
        Some(batch_id) => ProducerTransition::Three([
            ProducerEffect::ReleaseBatch { batch_id },
            payload,
            complete,
        ]),
        None => ProducerTransition::Two([payload, complete]),
    }
}
