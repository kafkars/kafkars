//! Fake driver port preserving exact enrollment requests and evidence ownership.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

use kafka_client_core::TransactionEpoch;

use super::port::{
    TransactionPartitionEnrollmentPort, TransactionPartitionEnrollmentPortCall,
    TransactionPartitionEnrollmentPortCallPoll, TransactionPartitionEnrollmentPortEvidence,
    TransactionPartitionEnrollmentPortFact, TransactionPartitionEnrollmentRequest,
};

#[derive(Debug, Eq, PartialEq)]
pub(super) struct RecordedRequest {
    pub(super) epoch: TransactionEpoch,
    pub(super) transactional_id: String,
    pub(super) producer_id: i64,
    pub(super) producer_epoch: i16,
    pub(super) topic: String,
    pub(super) partition: i32,
    pub(super) deadline: Instant,
}

pub(super) struct FakePort {
    fact: Option<TransactionPartitionEnrollmentPortFact>,
    epoch: Option<TransactionEpoch>,
    deadline_gated: bool,
    pub(super) requests: Vec<RecordedRequest>,
    pub(super) discarded: Arc<AtomicBool>,
}

impl FakePort {
    pub(super) fn accepted(
        epoch: TransactionEpoch,
        fact: TransactionPartitionEnrollmentPortFact,
    ) -> Self {
        Self {
            fact: Some(fact),
            epoch: Some(epoch),
            deadline_gated: false,
            requests: Vec::new(),
            discarded: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(super) fn refresh_stalled(
        epoch: TransactionEpoch,
        fact: TransactionPartitionEnrollmentPortFact,
    ) -> Self {
        Self {
            fact: Some(fact),
            epoch: Some(epoch),
            deadline_gated: true,
            requests: Vec::new(),
            discarded: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(super) fn rejected() -> Self {
        Self {
            fact: None,
            epoch: None,
            deadline_gated: false,
            requests: Vec::new(),
            discarded: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl TransactionPartitionEnrollmentPort for FakePort {
    fn submit(
        &mut self,
        request: TransactionPartitionEnrollmentRequest<'_>,
    ) -> Result<Box<dyn TransactionPartitionEnrollmentPortCall>, ()> {
        self.requests.push(RecordedRequest {
            epoch: request.epoch,
            transactional_id: request.transactional_id.to_owned(),
            producer_id: request.producer_id,
            producer_epoch: request.producer_epoch,
            topic: request.topic.to_string(),
            partition: request.partition,
            deadline: request.deadline,
        });
        let (Some(epoch), Some(fact)) = (self.epoch.take(), self.fact.take()) else {
            return Err(());
        };
        Ok(Box::new(FakeCall {
            evidence: Some(FakeEvidence {
                epoch,
                fact,
                discarded: Arc::clone(&self.discarded),
            }),
            deadline_gated: self.deadline_gated,
        }))
    }
}

struct FakeCall {
    evidence: Option<FakeEvidence>,
    deadline_gated: bool,
}

impl TransactionPartitionEnrollmentPortCall for FakeCall {
    fn poll(&mut self, deadline_elapsed: bool) -> TransactionPartitionEnrollmentPortCallPoll {
        if self.deadline_gated && !deadline_elapsed {
            return TransactionPartitionEnrollmentPortCallPoll::Pending;
        }
        self.evidence.take().map_or(
            TransactionPartitionEnrollmentPortCallPoll::Pending,
            |evidence| {
                if self.deadline_gated {
                    TransactionPartitionEnrollmentPortCallPoll::DeadlineElapsed(Box::new(evidence))
                } else {
                    TransactionPartitionEnrollmentPortCallPoll::Terminal(Box::new(evidence))
                }
            },
        )
    }

    fn discard_after_driver_shutdown(self: Box<Self>) {
        drop(self);
    }
}

struct FakeEvidence {
    epoch: TransactionEpoch,
    fact: TransactionPartitionEnrollmentPortFact,
    discarded: Arc<AtomicBool>,
}

impl TransactionPartitionEnrollmentPortEvidence for FakeEvidence {
    fn epoch(&self) -> TransactionEpoch {
        self.epoch
    }

    fn fact(&self) -> TransactionPartitionEnrollmentPortFact {
        self.fact
    }

    fn discard(self: Box<Self>) {
        self.discarded.store(true, Ordering::Release);
    }
}
