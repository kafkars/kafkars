//! Top-level dispatch from explicit producer facts to ownership transitions.

use crate::{ProducerInput, ProducerMachine, ProducerMachineError, ProducerTransition};

impl ProducerMachine {
    /// Applies one producer fact and returns ordered mechanism requests.
    pub fn apply(
        &mut self,
        input: ProducerInput,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        let Some(transition_effect_capacity) = self.transition_effect_capacity else {
            return Err(ProducerMachineError::Transition(
                crate::TransitionError::InvalidState,
            ));
        };
        let transition = match input {
            ProducerInput::AdmitExplicit {
                now,
                deadline,
                record,
            } => self.admit_explicit(now, deadline, record),
            ProducerInput::CancelRequested { operation_id } => self.cancel_requested(operation_id),
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
            ProducerInput::BatchMaterialized { execution, now } => {
                self.batch_materialized(execution, now)
            }
            ProducerInput::BatchMaterializationFailed { execution } => {
                self.materialization_failed(execution)
            }
            ProducerInput::DriverAccepted { execution } => self.driver_accepted(execution),
            ProducerInput::DriverRejected {
                execution,
                now,
                failure,
            } => self.driver_rejected(execution, now, failure),
            ProducerInput::BrokerSucceeded { execution, success } => {
                self.broker_succeeded(execution, success)
            }
            ProducerInput::BrokerFailed {
                execution,
                failure,
                delivery,
            } => self.broker_failed(execution, failure, delivery),
            ProducerInput::TransportFailed {
                execution,
                now,
                failure,
                delivery,
            } => self.transport_failed(execution, now, failure, delivery),
            ProducerInput::ExecutionUnavailable => self.execution_unavailable(),
            ProducerInput::FlushRequested => self.flush_requested(),
            ProducerInput::CloseRequested => self.close_requested(),
            ProducerInput::FlushCompletionReclaimed { flush_id } => {
                self.reclaim_flush(flush_id)?;
                Ok(ProducerTransition::none())
            }
            ProducerInput::DeadlineElapsed { operation_id, now } => {
                self.deadline_elapsed(operation_id, now)
            }
            ProducerInput::CompletionReclaimed { operation_id } => {
                self.reclaim_completion(operation_id)?;
                Ok(ProducerTransition::none())
            }
        }?;
        debug_assert!(
            transition.effects().len() <= transition_effect_capacity,
            "producer transition exceeded its construction-time effect bound"
        );
        Ok(transition)
    }
}
