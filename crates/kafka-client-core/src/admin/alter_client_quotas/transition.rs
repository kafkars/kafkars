//! Atomic client-quota alteration transitions and sole terminal assignment.

use std::collections::BTreeMap;

use crate::DeliveryStatus;

use super::{
    ALTER_CLIENT_QUOTAS_DIAGNOSTIC_BYTES, AlterClientQuotaOutcome, AlterClientQuotaResult,
    AlterClientQuotasBatch, AlterClientQuotasEffect, AlterClientQuotasFailure,
    AlterClientQuotasFailureKind, AlterClientQuotasInput, AlterClientQuotasMachine,
    AlterClientQuotasMachineError, AlterClientQuotasState, AlterClientQuotasTerminal,
    AlterClientQuotasTransition,
};

impl AlterClientQuotasMachine {
    /// Applies one normalized fact without hidden I/O, retry, or cancellation.
    pub fn apply(
        &mut self,
        input: AlterClientQuotasInput,
    ) -> Result<AlterClientQuotasTransition, AlterClientQuotasMachineError> {
        if self.state == AlterClientQuotasState::Completed {
            return Err(AlterClientQuotasMachineError::AlreadyCompleted);
        }
        match input {
            AlterClientQuotasInput::Start { now } => self.start(now),
            AlterClientQuotasInput::DriverAccepted => self.driver_accepted(),
            AlterClientQuotasInput::DriverRejected => self.finish_awaiting(
                AlterClientQuotasFailureKind::DriverRejected,
                DeliveryStatus::NotSent,
            ),
            AlterClientQuotasInput::DeadlineElapsed => self.finish_awaiting(
                AlterClientQuotasFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ),
            AlterClientQuotasInput::DriverDeadlineElapsed { delivery } => {
                self.finish_submitted(AlterClientQuotasFailureKind::DeadlineElapsed, delivery)
            }
            AlterClientQuotasInput::BrokerResponded { batch } => self.broker_responded(batch),
            AlterClientQuotasInput::ResponseTooLarge => self.finish_submitted(
                AlterClientQuotasFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ),
            AlterClientQuotasInput::ProtocolIncompatible { delivery } => {
                self.finish_submitted(AlterClientQuotasFailureKind::Compatibility, delivery)
            }
            AlterClientQuotasInput::TransportFailed { delivery } => {
                self.finish_submitted(AlterClientQuotasFailureKind::Transport, delivery)
            }
            AlterClientQuotasInput::InvalidResponse => self.finish_submitted(
                AlterClientQuotasFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ),
        }
    }

    fn start(
        &mut self,
        now: crate::Moment,
    ) -> Result<AlterClientQuotasTransition, AlterClientQuotasMachineError> {
        if self.state != AlterClientQuotasState::Ready {
            return Err(AlterClientQuotasMachineError::InvalidState);
        }
        if self.deadline.is_elapsed_at(now) {
            return Ok(self.finish_failure(
                AlterClientQuotasFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ));
        }
        self.state = AlterClientQuotasState::AwaitingDriver;
        Ok(AlterClientQuotasTransition::one(
            AlterClientQuotasEffect::Submit {
                operation_id: self.operation_id,
                deadline: self.deadline,
                plan: self.plan.clone(),
            },
        ))
    }

    fn driver_accepted(
        &mut self,
    ) -> Result<AlterClientQuotasTransition, AlterClientQuotasMachineError> {
        if self.state != AlterClientQuotasState::AwaitingDriver {
            return Err(AlterClientQuotasMachineError::InvalidState);
        }
        self.state = AlterClientQuotasState::Submitted;
        Ok(AlterClientQuotasTransition::none())
    }

    fn broker_responded(
        &mut self,
        batch: AlterClientQuotasBatch,
    ) -> Result<AlterClientQuotasTransition, AlterClientQuotasMachineError> {
        if self.state != AlterClientQuotasState::Submitted {
            return Err(AlterClientQuotasMachineError::InvalidState);
        }
        let Some(batch) = self.correlate_batch(batch) else {
            return Ok(self.finish_failure(
                AlterClientQuotasFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ));
        };
        Ok(self.finish(AlterClientQuotasTerminal::Altered(batch)))
    }

    fn correlate_batch(&self, batch: AlterClientQuotasBatch) -> Option<AlterClientQuotasBatch> {
        let (throttle_time_ms, outcomes) = batch.into_parts();
        if outcomes.len() != self.plan.entries().len() {
            return None;
        }
        let mut by_entity = BTreeMap::new();
        for outcome in outcomes {
            let (mut entity, result) = outcome.into_parts();
            if entity.validate_and_canonicalize().is_err()
                || !result_has_bounded_diagnostic(&result)
                || by_entity.insert(entity, result).is_some()
            {
                return None;
            }
        }
        let mut ordered = Vec::with_capacity(self.plan.entries().len());
        for entry in self.plan.entries() {
            let entity = entry.entity().clone();
            let result = by_entity.remove(&entity)?;
            ordered.push(match result {
                AlterClientQuotaResult::Altered => AlterClientQuotaOutcome::altered(entity),
                AlterClientQuotaResult::Failed(error) => {
                    AlterClientQuotaOutcome::failed(entity, error)
                }
            });
        }
        if !by_entity.is_empty() {
            return None;
        }
        Some(AlterClientQuotasBatch::new(throttle_time_ms, ordered))
    }

    fn finish_awaiting(
        &mut self,
        kind: AlterClientQuotasFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<AlterClientQuotasTransition, AlterClientQuotasMachineError> {
        if self.state != AlterClientQuotasState::AwaitingDriver {
            return Err(AlterClientQuotasMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_submitted(
        &mut self,
        kind: AlterClientQuotasFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<AlterClientQuotasTransition, AlterClientQuotasMachineError> {
        if self.state != AlterClientQuotasState::Submitted {
            return Err(AlterClientQuotasMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_failure(
        &mut self,
        kind: AlterClientQuotasFailureKind,
        delivery: DeliveryStatus,
    ) -> AlterClientQuotasTransition {
        self.finish(AlterClientQuotasTerminal::Failed(
            AlterClientQuotasFailure::new(kind, delivery),
        ))
    }

    fn finish(&mut self, terminal: AlterClientQuotasTerminal) -> AlterClientQuotasTransition {
        self.state = AlterClientQuotasState::Completed;
        AlterClientQuotasTransition::one(AlterClientQuotasEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}

fn result_has_bounded_diagnostic(result: &AlterClientQuotaResult) -> bool {
    match result {
        AlterClientQuotaResult::Altered => true,
        AlterClientQuotaResult::Failed(error) => error
            .message()
            .is_none_or(|message| message.len() <= ALTER_CLIENT_QUOTAS_DIAGNOSTIC_BYTES),
    }
}
