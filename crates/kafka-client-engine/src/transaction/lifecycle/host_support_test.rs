//! Shared fake transaction-end port and lifecycle-host fixtures.

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc::{Receiver, sync_channel},
    },
    time::{Duration, Instant},
};

use kafka_client_core::{
    Deadline, DeliveryStatus, ProducerRetryPolicy, TransactionEndBrokerFailureKind,
    TransactionEndFailure, TransactionEndFailureKind, TransactionEndMode, TransactionalOwnerId,
};

use crate::{
    clock::OperationDeadline,
    transaction::{
        completion::TransactionCompletionOwner, initialization::TransactionalOwnerParts,
    },
};

use super::{
    TransactionExecutionLimits,
    host::TransactionLifecycleHost,
    port::{
        TransactionEndPort, TransactionEndPortCall, TransactionEndPortCallPoll,
        TransactionEndPortTerminal, TransactionEndPortTerminalEvidence, TransactionEndRequest,
    },
};

pub(super) fn deadline(tick: u64) -> OperationDeadline {
    OperationDeadline::from_parts_for_test(
        Deadline::from_tick(tick),
        Instant::now() + Duration::from_secs(tick),
    )
}

pub(super) fn host() -> (
    TransactionLifecycleHost,
    Arc<AtomicBool>,
    Receiver<TransactionalOwnerId>,
    TransactionCompletionOwner,
) {
    host_with_policy(ProducerRetryPolicy::none())
}

pub(super) fn host_with_policy(
    retry_policy: ProducerRetryPolicy,
) -> (
    TransactionLifecycleHost,
    Arc<AtomicBool>,
    Receiver<TransactionalOwnerId>,
    TransactionCompletionOwner,
) {
    let (sender, receiver) = sync_channel(1);
    let active = Arc::new(AtomicBool::new(true));
    let completion = TransactionCompletionOwner::start()
        .unwrap_or_else(|error| panic!("transaction completion owner starts: {error:?}"));
    let parts = TransactionalOwnerParts::new(
        TransactionalOwnerId::from_raw(7),
        Arc::<str>::from("writer"),
        41,
        3,
        Arc::clone(&active),
        sender,
        completion
            .lifecycle_publisher()
            .unwrap_or_else(|error| panic!("publisher remains active: {error:?}")),
        completion
            .send_publisher()
            .unwrap_or_else(|error| panic!("send publisher remains active: {error:?}")),
        completion
            .offset_commit_publisher()
            .unwrap_or_else(|error| panic!("offset publisher remains active: {error:?}")),
    );
    let limits = TransactionExecutionLimits::try_new_with_retry_policy(
        8,
        1024,
        kafka_client_core::CompressionPolicy::None,
        retry_policy,
    )
    .unwrap_or_else(|| panic!("limits"));
    let host = TransactionLifecycleHost::try_new(parts, limits)
        .unwrap_or_else(|(error, _parts)| panic!("transaction lifecycle starts: {error:?}"));
    (host, active, receiver, completion)
}

pub(super) fn assert_released(active: &Arc<AtomicBool>, release: &Receiver<TransactionalOwnerId>) {
    assert!(!active.load(Ordering::Acquire));
    assert_eq!(release.try_recv().map(TransactionalOwnerId::get), Ok(7));
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct RecordedRequest {
    pub(super) transactional_id: String,
    pub(super) producer_id: i64,
    pub(super) producer_epoch: i16,
    pub(super) mode: TransactionEndMode,
    pub(super) deadline: Instant,
}

pub(super) struct FakePort {
    pub(super) requests: Arc<Mutex<Vec<RecordedRequest>>>,
    terminals: Vec<TransactionEndPortTerminal>,
}

impl FakePort {
    pub(super) fn succeeding() -> Self {
        Self {
            requests: Arc::new(Mutex::new(Vec::new())),
            terminals: vec![TransactionEndPortTerminal::Succeeded],
        }
    }

    pub(super) fn retrying_once() -> Self {
        Self {
            requests: Arc::new(Mutex::new(Vec::new())),
            terminals: vec![
                TransactionEndPortTerminal::RetryableCoordinatorLoss(
                    TransactionEndFailure::broker(
                        TransactionEndMode::Commit,
                        TransactionEndBrokerFailureKind::Coordinator,
                        DeliveryStatus::PossiblySent,
                        core::num::NonZeroI16::new(14)
                            .unwrap_or_else(|| panic!("coordinator code is nonzero")),
                    ),
                ),
                TransactionEndPortTerminal::Succeeded,
            ],
        }
    }

    pub(super) fn retrying_then_rejecting() -> Self {
        let mut port = Self::retrying_once();
        port.terminals.pop();
        port
    }

    pub(super) fn failed(failure: TransactionEndFailure) -> Self {
        Self {
            requests: Arc::new(Mutex::new(Vec::new())),
            terminals: vec![TransactionEndPortTerminal::Failed(failure)],
        }
    }

    pub(super) fn first_mode(&self) -> TransactionEndMode {
        self.requests
            .lock()
            .unwrap_or_else(|error| panic!("request lock: {error:?}"))[0]
            .mode
    }
}

impl TransactionEndPort for FakePort {
    fn submit(
        &mut self,
        request: TransactionEndRequest<'_>,
    ) -> Result<Box<dyn TransactionEndPortCall>, TransactionEndFailure> {
        self.requests
            .lock()
            .unwrap_or_else(|error| panic!("request lock: {error:?}"))
            .push(RecordedRequest {
                transactional_id: request.transactional_id.to_owned(),
                producer_id: request.producer_id,
                producer_epoch: request.producer_epoch,
                mode: request.mode,
                deadline: request.deadline,
            });
        if self.terminals.is_empty() {
            return Err(TransactionEndFailure::local(
                request.mode,
                TransactionEndFailureKind::DriverRejected,
                DeliveryStatus::NotSent,
            ));
        }
        Ok(Box::new(FakeCall {
            terminal: Some(self.terminals.remove(0)),
            mode: request.mode,
        }))
    }
}

struct FakeCall {
    terminal: Option<TransactionEndPortTerminal>,
    mode: TransactionEndMode,
}

impl TransactionEndPortCall for FakeCall {
    fn poll(&mut self, _deadline_elapsed: bool) -> TransactionEndPortCallPoll {
        self.terminal
            .take()
            .map_or(TransactionEndPortCallPoll::Pending, |terminal| {
                TransactionEndPortCallPoll::Terminal(Box::new(FakeEvidence(terminal)))
            })
    }

    fn recover_after_driver_shutdown(self: Box<Self>) -> TransactionEndFailure {
        TransactionEndFailure::local(
            self.mode,
            TransactionEndFailureKind::DriverClosed,
            DeliveryStatus::PossiblySent,
        )
    }
}

struct FakeEvidence(TransactionEndPortTerminal);

impl TransactionEndPortTerminalEvidence for FakeEvidence {
    fn terminal(&self) -> TransactionEndPortTerminal {
        self.0
    }

    fn discard(self: Box<Self>) {
        drop(self);
    }
}
