//! Atomic reservation and FIFO transfer for one waiting producer caller.

mod failure;

use std::sync::Arc;

use kafka_client_core::{
    ByteCount, Moment, ProducerInput, ProducerMachineError, ProducerWaitingAdmissionError,
};

use crate::{
    ProducerDeliveryObserver,
    clock::OperationDeadline,
    completion::{CompletionId, CompletionObserver},
};

use super::{WaitingToken, model::WaitingEntry};
use crate::producer::{
    ProducerHost, ProducerHostInvariantError, ProducerRecord, ProducerRejectionReason,
};
pub(crate) use failure::ProducerWaitingAdmissionFailure;
use failure::rejected;

/// Accepted waiting ownership paired with its sole terminal observer.
pub(crate) struct AdmittedWaiting {
    id: kafka_client_core::ProducerWaiterId,
    observer: ProducerDeliveryObserver,
    token: Arc<WaitingToken>,
    fault: Option<ProducerHostInvariantError>,
}

impl AdmittedWaiting {
    pub(crate) fn into_parts(
        self,
    ) -> (
        kafka_client_core::ProducerWaiterId,
        ProducerDeliveryObserver,
        Arc<WaitingToken>,
    ) {
        (self.id, self.observer, self.token)
    }

    pub(in crate::producer) fn into_port_parts(
        self,
    ) -> (
        kafka_client_core::ProducerWaiterId,
        ProducerDeliveryObserver,
        Arc<WaitingToken>,
        Option<ProducerHostInvariantError>,
    ) {
        (self.id, self.observer, self.token, self.fault)
    }
}

impl ProducerHost {
    #[expect(
        clippy::result_large_err,
        clippy::too_many_lines,
        reason = "admission returns intact record ownership and commits each bounded owner in order"
    )]
    pub(crate) fn try_admit_waiting(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
        record: ProducerRecord,
    ) -> Result<AdmittedWaiting, ProducerWaitingAdmissionFailure> {
        if let Some(error) = self.poison_reason() {
            return Err(rejected(
                record,
                ProducerRejectionReason::HostPoisoned(error),
            ));
        }
        let retained_bytes = match record.retained_bytes() {
            Ok(bytes) => bytes,
            Err(error) => return Err(rejected(record, ProducerRejectionReason::Store(error))),
        };
        let Ok(retained_bytes_core) = u64::try_from(retained_bytes) else {
            return Err(rejected(
                record,
                ProducerRejectionReason::Waiting(ProducerWaitingAdmissionError::ByteCountOverflow),
            ));
        };
        let (completion_id, observer) = match self.completions.reserve() {
            Ok(reserved) => reserved,
            Err(error) => {
                return Err(rejected(record, ProducerRejectionReason::Completion(error)));
            }
        };
        let topic_id = match self.store.retain_waiting_topic(Arc::clone(record.topic())) {
            Ok(topic_id) => topic_id,
            Err(error) => {
                let rollback = self.completions.rollback_reservation(completion_id);
                drop(observer);
                return match rollback {
                    Ok(()) => Err(rejected(record, ProducerRejectionReason::Store(error))),
                    Err(error) => Err(ProducerWaitingAdmissionFailure::Invariant {
                        error: self.poison(ProducerHostInvariantError::Completion(error)),
                        record,
                    }),
                };
            }
        };
        let id = match self.waiting_policy.admit(
            now,
            deadline.core(),
            ByteCount::new(retained_bytes_core),
        ) {
            Ok(id) => id,
            Err(error) => {
                let rollback = self.completions.rollback_reservation(completion_id);
                let topic_rollback = self.store.release_waiting_topic(topic_id);
                drop(observer);
                return match (rollback, topic_rollback) {
                    (Ok(()), Ok(())) => {
                        Err(rejected(record, ProducerRejectionReason::Waiting(error)))
                    }
                    (Err(error), _) => Err(ProducerWaitingAdmissionFailure::Invariant {
                        error: self.poison(ProducerHostInvariantError::Completion(error)),
                        record,
                    }),
                    (Ok(()), Err(error)) => Err(ProducerWaitingAdmissionFailure::Invariant {
                        error: self.poison(ProducerHostInvariantError::Store(error)),
                        record,
                    }),
                };
            }
        };
        let token = Arc::new(WaitingToken::new());
        let transition = match self.core.apply(ProducerInput::AdmitWaiting {
            now,
            deadline: deadline.core(),
            retained_bytes: ByteCount::new(retained_bytes_core),
        }) {
            Ok(transition) => transition,
            Err(ProducerMachineError::Admission(reason)) => {
                let record = self.rollback_waiting_before_core(
                    id,
                    completion_id,
                    observer,
                    topic_id,
                    record,
                )?;
                return Err(rejected(record, ProducerRejectionReason::Core(reason)));
            }
            Err(error) => {
                let record = self.rollback_waiting_before_core(
                    id,
                    completion_id,
                    observer,
                    topic_id,
                    record,
                )?;
                return Err(ProducerWaitingAdmissionFailure::Invariant {
                    error: self.poison(ProducerHostInvariantError::Core(error)),
                    record,
                });
            }
        };
        let Some(operation_id) = transition.admitted_operation_id() else {
            let error = self.poison(ProducerHostInvariantError::MissingAdmissionIdentity);
            let _removed = self.waiting_policy.remove(id);
            let _released = self.store.release_waiting_topic(topic_id);
            if let Ok(mut state) = token.lock() {
                *state = super::model::WaitingTokenState::Settled;
            }
            drop(record);
            return Ok(AdmittedWaiting {
                id,
                observer: ProducerDeliveryObserver::from_completion(observer),
                token,
                fault: Some(error),
            });
        };
        self.waiting.push(WaitingEntry {
            id,
            operation_id,
            record,
            topic_id,
            token: Arc::clone(&token),
        });
        let fault = self
            .bindings
            .bind_waiting(operation_id, completion_id, deadline)
            .err()
            .map(|error| self.poison(ProducerHostInvariantError::Binding(error)));
        Ok(AdmittedWaiting {
            id,
            observer: ProducerDeliveryObserver::from_completion(observer),
            token,
            fault,
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "rollback failure returns exact record ownership to the caller"
    )]
    fn rollback_waiting_before_core(
        &mut self,
        id: kafka_client_core::ProducerWaiterId,
        completion_id: CompletionId,
        observer: CompletionObserver<crate::producer::terminal::ProducerTerminal>,
        topic_id: kafka_client_core::TopicId,
        record: ProducerRecord,
    ) -> Result<ProducerRecord, ProducerWaitingAdmissionFailure> {
        let waiting = self.waiting_policy.remove(id).is_some();
        let completion = self.completions.rollback_reservation(completion_id);
        let topic = self.store.release_waiting_topic(topic_id);
        drop(observer);
        match (waiting, completion, topic) {
            (true, Ok(()), Ok(())) => Ok(record),
            (false, _, _) => Err(ProducerWaitingAdmissionFailure::Invariant {
                error: self.poison(ProducerHostInvariantError::WaitingOwnership),
                record,
            }),
            (_, Err(error), _) => Err(ProducerWaitingAdmissionFailure::Invariant {
                error: self.poison(ProducerHostInvariantError::Completion(error)),
                record,
            }),
            (_, Ok(()), Err(error)) => Err(ProducerWaitingAdmissionFailure::Invariant {
                error: self.poison(ProducerHostInvariantError::Store(error)),
                record,
            }),
        }
    }
}
