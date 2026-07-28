//! Scripted Produce submission, retained evidence, and discard-order fixture.

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use kafka_client_core::{TransactionSendAttempt, TransactionSendId};

use crate::driver::{
    DriverOwner,
    transaction_produce::{TransactionProduceRouteRefreshPoll, TransactionProduceTerminalFact},
};

use super::{
    super::port::{
        TransactionSendProduceCall, TransactionSendProduceEvidence, TransactionSendProducePort,
        TransactionSendProduceRequest, TransactionSendProduceSubmissionFailure,
    },
    FakeAggregate,
};

pub(in crate::transaction::send) struct FakeProducePort {
    pub(in crate::transaction::send) fact: Option<TransactionProduceTerminalFact>,
    pub(in crate::transaction::send) replacement_fact: Option<TransactionProduceTerminalFact>,
    pub(in crate::transaction::send) submit_failure:
        Option<TransactionSendProduceSubmissionFailure>,
    pub(in crate::transaction::send) observed_deadline: Option<crate::clock::OperationDeadline>,
    pub(in crate::transaction::send) observed_transactional_id: Option<String>,
    pub(in crate::transaction::send) observed_deadlines: Vec<crate::clock::OperationDeadline>,
    pub(in crate::transaction::send) observed_transactional_ids: Vec<String>,
    pub(in crate::transaction::send) observed_attempts: Vec<TransactionSendAttempt>,
    pub(in crate::transaction::send) observed_records: Vec<Bytes>,
    pub(in crate::transaction::send) submit_count: usize,
    pub(in crate::transaction::send) terminal_attempt: Option<TransactionSendAttempt>,
    pub(in crate::transaction::send) replacement_terminal_attempt: Option<TransactionSendAttempt>,
    pub(in crate::transaction::send) route_refresh_polls:
        Arc<Mutex<VecDeque<TransactionProduceRouteRefreshPoll>>>,
    pub(in crate::transaction::send) log: Arc<Mutex<Vec<&'static str>>>,
}

impl FakeProducePort {
    pub(in crate::transaction::send) fn success(
        aggregate: &FakeAggregate,
        send_id: TransactionSendId,
    ) -> Self {
        Self {
            fact: Some(TransactionProduceTerminalFact::Succeeded {
                epoch: aggregate.epoch,
                send_id,
                success: kafka_client_core::ProducerBatchSuccess::new(42, None, None),
            }),
            replacement_fact: None,
            submit_failure: None,
            observed_deadline: None,
            observed_transactional_id: None,
            observed_deadlines: Vec::new(),
            observed_transactional_ids: Vec::new(),
            observed_attempts: Vec::new(),
            observed_records: Vec::new(),
            submit_count: 0,
            terminal_attempt: None,
            replacement_terminal_attempt: None,
            route_refresh_polls: Arc::new(Mutex::new(VecDeque::new())),
            log: Arc::clone(&aggregate.log),
        }
    }
}

impl TransactionSendProducePort for FakeProducePort {
    fn submit(
        &mut self,
        request: TransactionSendProduceRequest<'_>,
    ) -> Result<Box<dyn TransactionSendProduceCall>, TransactionSendProduceSubmissionFailure> {
        self.observed_deadline = Some(request.deadline);
        self.observed_transactional_id = Some(request.transactional_id.to_owned());
        self.observed_deadlines.push(request.deadline);
        self.observed_transactional_ids
            .push(request.transactional_id.to_owned());
        self.observed_attempts.push(request.attempt);
        self.observed_records
            .push(request.materialized.encoded_records().clone());
        self.submit_count = self.submit_count.saturating_add(1);
        if let Some(failure) = self.submit_failure {
            return Err(failure);
        }
        let fact = if self.submit_count == 1 {
            self.fact.take()
        } else {
            self.replacement_fact.take()
        };
        let terminal_attempt = if self.submit_count == 1 {
            self.terminal_attempt.take()
        } else {
            self.replacement_terminal_attempt.take()
        };
        Ok(Box::new(FakeProduceCall {
            attempt: terminal_attempt.unwrap_or(request.attempt),
            fact,
            route_refresh_polls: Arc::clone(&self.route_refresh_polls),
            log: Arc::clone(&self.log),
        }))
    }
}

struct FakeProduceCall {
    attempt: TransactionSendAttempt,
    fact: Option<TransactionProduceTerminalFact>,
    route_refresh_polls: Arc<Mutex<VecDeque<TransactionProduceRouteRefreshPoll>>>,
    log: Arc<Mutex<Vec<&'static str>>>,
}

impl TransactionSendProduceCall for FakeProduceCall {
    fn try_terminal(&mut self) -> Option<Box<dyn TransactionSendProduceEvidence>> {
        self.fact.take().map(|fact| {
            Box::new(FakeProduceEvidence {
                attempt: self.attempt,
                fact,
                route_refresh_polls: Arc::clone(&self.route_refresh_polls),
                log: Arc::clone(&self.log),
            }) as Box<_>
        })
    }

    fn recover_after_driver_shutdown(self: Box<Self>) -> Box<dyn TransactionSendProduceEvidence> {
        Box::new(FakeProduceEvidence {
            attempt: self.attempt,
            fact: self
                .fact
                .unwrap_or_else(|| panic!("recovery fact retained")),
            route_refresh_polls: Arc::clone(&self.route_refresh_polls),
            log: Arc::clone(&self.log),
        })
    }
}

struct FakeProduceEvidence {
    attempt: TransactionSendAttempt,
    fact: TransactionProduceTerminalFact,
    route_refresh_polls: Arc<Mutex<VecDeque<TransactionProduceRouteRefreshPoll>>>,
    log: Arc<Mutex<Vec<&'static str>>>,
}

impl TransactionSendProduceEvidence for FakeProduceEvidence {
    fn attempt(&self) -> TransactionSendAttempt {
        self.attempt
    }

    fn fact(&self) -> TransactionProduceTerminalFact {
        self.fact
    }

    fn poll_route_refresh(&mut self, _driver: &DriverOwner) -> TransactionProduceRouteRefreshPoll {
        self.route_refresh_polls
            .lock()
            .unwrap_or_else(|error| panic!("route refresh polls: {error:?}"))
            .pop_front()
            .unwrap_or(TransactionProduceRouteRefreshPoll::Failed)
    }

    fn discard(self: Box<Self>) {
        self.log
            .lock()
            .unwrap_or_else(|error| panic!("log: {error:?}"))
            .push("discard");
    }
}
