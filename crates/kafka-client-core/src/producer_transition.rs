//! Top-level dispatch from explicit producer facts to ownership transitions.

use crate::{ProducerInput, ProducerMachine, ProducerMachineError, ProducerTransition};

impl ProducerMachine {
    /// Applies one producer fact and returns ordered mechanism requests.
    pub fn apply(
        &mut self,
        input: ProducerInput,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        match input {
            ProducerInput::AdmitExplicit {
                now,
                deadline,
                record,
            } => self.admit_explicit(now, deadline, record),
            ProducerInput::RecordAccumulated {
                operation_id,
                batch_id,
                accumulator_bytes,
                now,
            } => self.record_accumulated(operation_id, batch_id, accumulator_bytes, now),
            ProducerInput::BatchTimerFired {
                batch_id,
                generation,
                now,
            } => self.batch_timer_fired(batch_id, generation, now),
            ProducerInput::BatchMaterialized { batch_id, now } => {
                self.batch_materialized(batch_id, now)
            }
            ProducerInput::BatchMaterializationFailed { batch_id } => {
                self.materialization_failed(batch_id)
            }
            ProducerInput::DriverAccepted { batch_id } => self.driver_accepted(batch_id),
            ProducerInput::DriverRejected { batch_id } => self.driver_rejected(batch_id),
            ProducerInput::BrokerSucceeded { batch_id, success } => {
                self.broker_succeeded(batch_id, success)
            }
            ProducerInput::BrokerFailed {
                batch_id,
                failure,
                delivery,
            } => self.broker_failed(batch_id, failure, delivery),
            ProducerInput::TransportFailed { batch_id, delivery } => {
                self.transport_failed(batch_id, delivery)
            }
            ProducerInput::ExecutionUnavailable => self.execution_unavailable(),
            ProducerInput::DeadlineElapsed { operation_id, now } => {
                self.deadline_elapsed(operation_id, now)
            }
            ProducerInput::CompletionReclaimed { operation_id } => {
                self.reclaim_completion(operation_id)?;
                Ok(ProducerTransition::none())
            }
        }
    }
}
