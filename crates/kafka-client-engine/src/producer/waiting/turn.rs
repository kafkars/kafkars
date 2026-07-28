//! Bounded cancellation, timeout, close, publication, and FIFO promotion.

use kafka_client_core::{
    AdmissionRejection, Moment, ProducerInput, ProducerMachineError, ProducerWaitingTerminal,
};

use super::model::{ProducerWaitingStats, WaitingTokenState};
use crate::producer::{ProducerHost, ProducerHostInvariantError};

pub(in crate::producer) struct WaitingTurn {
    pub(in crate::producer) progressed: usize,
    pub(in crate::producer) blocked: bool,
}

impl ProducerHost {
    pub(in crate::producer) fn waiting_stats(&self) -> ProducerWaitingStats {
        ProducerWaitingStats {
            records: self.waiting_policy.len(),
            bytes: self.waiting_policy.retained_bytes(),
            terminal_bindings: self.bindings.waiting_terminal_len(),
        }
    }

    pub(in crate::producer) fn waiting_next_deadline(&self) -> Option<kafka_client_core::Deadline> {
        self.waiting_policy.next_deadline()
    }

    pub(in crate::producer) fn drive_waiting(
        &mut self,
        now: Moment,
        limit: usize,
    ) -> Result<WaitingTurn, ProducerHostInvariantError> {
        let mut progressed = 0;
        let mut blocked = false;
        while progressed < limit {
            if let Some(id) = self.waiting.first_cancelled() {
                self.settle_waiter(id, ProducerWaitingTerminal::Cancelled)?;
                progressed += 1;
                continue;
            }
            if let Some(waiter) = self.waiting_policy.first_elapsed(now) {
                self.settle_waiter(waiter.id(), ProducerWaitingTerminal::DeadlineElapsed)?;
                progressed += 1;
                continue;
            }
            if !self.waiting_policy.admission_is_open() {
                let Some(waiter) = self.waiting_policy.front() else {
                    break;
                };
                self.settle_waiter(waiter.id(), ProducerWaitingTerminal::Closed)?;
                progressed += 1;
                continue;
            }
            let Some(id) = self
                .waiting_policy
                .front()
                .map(kafka_client_core::ProducerWaiter::id)
            else {
                break;
            };
            if self.waiting.front_needs_partition(id)? {
                blocked = true;
                break;
            }
            if self.promote_waiter(now, id)? {
                progressed += 1;
            } else {
                blocked = true;
                break;
            }
        }
        Ok(WaitingTurn {
            progressed,
            blocked,
        })
    }

    #[expect(
        clippy::too_many_lines,
        reason = "linear token, record, core, and completion ownership transitions stay adjacent"
    )]
    fn promote_waiter(
        &mut self,
        now: Moment,
        id: kafka_client_core::ProducerWaiterId,
    ) -> Result<bool, ProducerHostInvariantError> {
        let Some(entry) = self.waiting.remove(id) else {
            return Err(self.poison(ProducerHostInvariantError::WaitingOwnership));
        };
        let token = std::sync::Arc::clone(&entry.token);
        let mut token_state = token
            .lock()
            .map_err(|_| self.poison(ProducerHostInvariantError::WaitingToken))?;
        if token.cancellation_requested() {
            drop(token_state);
            self.waiting.restore_front(entry);
            self.settle_waiter(id, ProducerWaitingTerminal::Cancelled)?;
            return Ok(true);
        }
        *token_state = WaitingTokenState::Promoting;
        let reservation = match self.store.reserve(entry.record) {
            Ok(reservation) => reservation,
            Err(error) => {
                let reason = error.reason();
                let record = error.into_record();
                *token_state = WaitingTokenState::Waiting;
                drop(token_state);
                self.waiting
                    .restore_front(super::model::WaitingEntry { record, ..entry });
                return match reason {
                    crate::producer::ProducerStoreError::RecordCapacity
                    | crate::producer::ProducerStoreError::ByteCapacity => Ok(false),
                    _ => Err(self.poison(ProducerHostInvariantError::Store(reason))),
                };
            }
        };
        let facts = reservation.facts();
        let transition = match self.core.apply(ProducerInput::PromoteWaiting {
            operation_id: entry.operation_id,
            now,
            record: facts,
        }) {
            Ok(transition) => transition,
            Err(ProducerMachineError::Admission(
                AdmissionRejection::ByteCapacity | AdmissionRejection::AccumulatorPending,
            )) => {
                let (record, cleanup) = self.store.rollback(reservation).into_parts();
                if let Err(error) = cleanup {
                    return Err(self.poison(ProducerHostInvariantError::Store(error)));
                }
                *token_state = WaitingTokenState::Waiting;
                drop(token_state);
                self.waiting
                    .restore_front(super::model::WaitingEntry { record, ..entry });
                return Ok(false);
            }
            Err(ProducerMachineError::Admission(AdmissionRejection::DeadlineElapsed)) => {
                let (record, cleanup) = self.store.rollback(reservation).into_parts();
                if let Err(error) = cleanup {
                    return Err(self.poison(ProducerHostInvariantError::Store(error)));
                }
                *token_state = WaitingTokenState::Waiting;
                drop(token_state);
                self.waiting
                    .restore_front(super::model::WaitingEntry { record, ..entry });
                self.settle_waiter(id, ProducerWaitingTerminal::DeadlineElapsed)?;
                return Ok(true);
            }
            Err(ProducerMachineError::Admission(AdmissionRejection::Closed)) => {
                let (record, cleanup) = self.store.rollback(reservation).into_parts();
                if let Err(error) = cleanup {
                    return Err(self.poison(ProducerHostInvariantError::Store(error)));
                }
                *token_state = WaitingTokenState::Waiting;
                drop(token_state);
                self.waiting
                    .restore_front(super::model::WaitingEntry { record, ..entry });
                self.settle_waiter(id, ProducerWaitingTerminal::Closed)?;
                return Ok(true);
            }
            Err(error) => {
                let (record, cleanup) = self.store.rollback(reservation).into_parts();
                *token_state = WaitingTokenState::Waiting;
                drop(token_state);
                self.waiting
                    .restore_front(super::model::WaitingEntry { record, ..entry });
                if let Err(error) = cleanup {
                    return Err(self.poison(ProducerHostInvariantError::Store(error)));
                }
                return Err(self.poison(ProducerHostInvariantError::Core(error)));
            }
        };
        let Some(operation_id) = transition.admitted_operation_id() else {
            let (record, cleanup) = self.store.rollback(reservation).into_parts();
            *token_state = WaitingTokenState::Waiting;
            drop(token_state);
            self.waiting
                .restore_front(super::model::WaitingEntry { record, ..entry });
            if let Err(error) = cleanup {
                return Err(self.poison(ProducerHostInvariantError::Store(error)));
            }
            return Err(self.poison(ProducerHostInvariantError::MissingAdmissionIdentity));
        };
        if operation_id != entry.operation_id {
            let (record, cleanup) = self.store.rollback(reservation).into_parts();
            *token_state = WaitingTokenState::Waiting;
            drop(token_state);
            self.waiting
                .restore_front(super::model::WaitingEntry { record, ..entry });
            if let Err(error) = cleanup {
                return Err(self.poison(ProducerHostInvariantError::Store(error)));
            }
            return Err(self.poison(ProducerHostInvariantError::WaitingOwnership));
        }
        let committed = self
            .store
            .commit(reservation)
            .map_err(|error| self.poison(ProducerHostInvariantError::Store(error)))?;
        if committed != facts {
            return Err(self.poison(ProducerHostInvariantError::CommittedFactsMismatch));
        }
        if facts.topic_id() != entry.topic_id {
            return Err(self.poison(ProducerHostInvariantError::WaitingOwnership));
        }
        self.store
            .release_waiting_topic(entry.topic_id)
            .map_err(|error| self.poison(ProducerHostInvariantError::Store(error)))?;
        if self.waiting_policy.remove(id).is_none() {
            return Err(self.poison(ProducerHostInvariantError::WaitingOwnership));
        }
        *token_state = WaitingTokenState::Accepted(operation_id);
        let cancel_after_promotion = token.cancellation_requested();
        drop(token_state);
        self.interpret_transition(now, transition)?;
        if cancel_after_promotion {
            self.try_cancel_operation(operation_id)
                .map_err(|error| match error {
                    crate::producer::cancellation::ProducerHostCancelError::Invariant(error)
                    | crate::producer::cancellation::ProducerHostCancelError::HostUnavailable(error) => {
                        self.poison(error)
                    }
                    crate::producer::cancellation::ProducerHostCancelError::ExecutionGenerationExhausted => {
                        self.poison(ProducerHostInvariantError::WaitingOwnership)
                    }
                })?;
        }
        Ok(true)
    }
}
