//! Top-level dispatch from explicit producer facts to ownership transitions.

use crate::{ProducerInput, ProducerMachine, ProducerMachineError, ProducerTransition};

impl ProducerMachine {
    /// Applies one producer fact and returns ordered mechanism requests.
    #[allow(
        clippy::too_many_lines,
        reason = "the explicit dispatcher keeps every producer input and terminal path visible"
    )]
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
            ProducerInput::ProducerIdentityAcquired {
                generation,
                producer_id,
                producer_epoch,
                now,
            } => self.producer_identity_acquired(generation, producer_id, producer_epoch, now),
            ProducerInput::ProducerIdentityFailed {
                generation,
                broker_code,
                now,
            } => self.producer_identity_failed(generation, broker_code, now),
            ProducerInput::ProducerIdentityRetryDue { schedule, now } => {
                self.producer_identity_retry_due(schedule, now)
            }
            ProducerInput::ProducerIdentityDeadlineElapsed { generation, now } => {
                self.producer_identity_deadline_elapsed(generation, now)
            }
            ProducerInput::ProducerIdentityRequestUnavailable { generation, now } => {
                self.producer_identity_request_unavailable(generation, now)
            }
            ProducerInput::ProducerIdentityRequestFailed { generation, now } => {
                self.producer_identity_request_failed(generation, now)
            }
            ProducerInput::AdmitExplicit {
                now,
                deadline,
                record,
            } => self.admit_explicit(now, deadline, record),
            ProducerInput::AdmitWaiting {
                now,
                deadline,
                retained_bytes,
            } => self.admit_waiting(now, deadline, retained_bytes),
            ProducerInput::PromoteWaiting {
                operation_id,
                now,
                record,
            } => self.promote_waiting(operation_id, now, record),
            ProducerInput::WaitingTerminal {
                operation_id,
                terminal,
            } => self.waiting_terminal(operation_id, terminal),
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
                now,
                failure,
                delivery,
                route_refreshed,
            } => self.broker_failed(execution, now, failure, delivery, route_refreshed),
            ProducerInput::RouteRefreshDeadlineElapsed {
                execution,
                now,
                delivery,
            }
            | ProducerInput::DriverDeadlineElapsed {
                execution,
                now,
                delivery,
            } => self.attempt_deadline_elapsed(execution, now, delivery),
            ProducerInput::TransportFailed {
                execution,
                now,
                failure,
                delivery,
                route_refreshed,
            } => self.transport_failed(execution, now, failure, delivery, route_refreshed),
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
