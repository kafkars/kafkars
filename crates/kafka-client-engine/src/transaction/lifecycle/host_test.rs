//! Private transaction lifecycle execution and completion scenarios.

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc::{Receiver, sync_channel},
    },
    time::{Duration, Instant},
};

use kafka_client_core::{
    Deadline, Moment, ProducerRetryPolicy, TransactionEndMode, TransactionLifecycleTerminal,
    TransactionalOwnerId,
};

use crate::{
    clock::OperationDeadline,
    transaction::{
        completion::TransactionCompletionOwner, initialization::TransactionalOwnerParts,
    },
};

use super::{
    host::{TransactionLifecycleHost, TransactionLifecycleTurn},
    port::{
        TransactionEndPort, TransactionEndPortCall, TransactionEndPortCallPoll,
        TransactionEndPortTerminal, TransactionEndPortTerminalEvidence, TransactionEndRequest,
    },
};

#[test]
fn commit_uses_original_deadline_and_publishes_one_terminal() {
    let (mut host, active, release, _completion) = host();
    let epoch = host
        .begin()
        .unwrap_or_else(|error| panic!("transaction begins locally: {error:?}"));
    let deadline = deadline(31);
    let observer = host
        .commit(epoch, deadline)
        .unwrap_or_else(|error| panic!("commit is admitted: {error:?}"));
    let mut port = FakePort::succeeding();

    assert_eq!(
        host.turn_with(&mut port),
        Ok(TransactionLifecycleTurn::Progress)
    );
    assert_eq!(
        port.requests
            .lock()
            .unwrap_or_else(|error| panic!("request lock: {error:?}"))
            .as_slice(),
        &[RecordedRequest {
            transactional_id: "writer".to_owned(),
            producer_id: 41,
            producer_epoch: 3,
            mode: TransactionEndMode::Commit,
            deadline: deadline.transport(),
        }]
    );
    drive_three(&mut host, &mut port);
    assert_eq!(observer.wait(), Ok(TransactionLifecycleTerminal::Committed));

    host.idle_owner_lost()
        .unwrap_or_else(|error| panic!("idle owner releases: {error:?}"));
    assert_released(&active, &release);
}

#[test]
fn refreshed_coordinator_rejection_retries_under_the_original_deadline() {
    let retry_policy = ProducerRetryPolicy::try_fixed(1, 1)
        .unwrap_or_else(|_| panic!("one bounded retry with positive backoff"));
    let (mut host, _active, _release, _completion) = host_with_policy(retry_policy);
    let epoch = host
        .begin()
        .unwrap_or_else(|error| panic!("transaction begins: {error:?}"));
    let deadline = deadline(31);
    let observer = host
        .commit(epoch, deadline)
        .unwrap_or_else(|error| panic!("commit is admitted: {error:?}"));
    let mut port = FakePort::retrying_once();

    assert_eq!(
        host.turn_with_at(Moment::from_tick(0), &mut port),
        Ok(TransactionLifecycleTurn::Progress)
    );
    assert_eq!(
        host.turn_with_at(Moment::from_tick(0), &mut port),
        Ok(TransactionLifecycleTurn::Progress)
    );
    assert_eq!(
        host.turn_with_at(Moment::from_tick(0), &mut port),
        Ok(TransactionLifecycleTurn::Idle)
    );
    for _ in 0..3 {
        assert_eq!(
            host.turn_with_at(Moment::from_tick(1), &mut port),
            Ok(TransactionLifecycleTurn::Progress)
        );
    }
    assert_eq!(observer.wait(), Ok(TransactionLifecycleTerminal::Committed));
    let requests = port
        .requests
        .lock()
        .unwrap_or_else(|error| panic!("request lock: {error:?}"));
    assert_eq!(requests.len(), 2);
    assert!(
        requests
            .iter()
            .all(|request| request.deadline == deadline.transport())
    );
}

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

fn host_with_policy(
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
    );
    let limits = super::TransactionExecutionLimits::try_new_with_retry_policy(
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

fn drive_three(host: &mut TransactionLifecycleHost, port: &mut FakePort) {
    for _ in 0..3 {
        host.turn_with(port)
            .unwrap_or_else(|error| panic!("host turn: {error:?}"));
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RecordedRequest {
    transactional_id: String,
    producer_id: i64,
    producer_epoch: i16,
    mode: TransactionEndMode,
    deadline: Instant,
}

pub(super) struct FakePort {
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
    terminals: Vec<TransactionEndPortTerminal>,
}

impl FakePort {
    pub(super) fn succeeding() -> Self {
        Self {
            requests: Arc::new(Mutex::new(Vec::new())),
            terminals: vec![TransactionEndPortTerminal::Succeeded],
        }
    }

    fn retrying_once() -> Self {
        Self {
            requests: Arc::new(Mutex::new(Vec::new())),
            terminals: vec![
                TransactionEndPortTerminal::RetryableCoordinatorLoss,
                TransactionEndPortTerminal::Succeeded,
            ],
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
    ) -> Result<Box<dyn TransactionEndPortCall>, ()> {
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
            return Err(());
        }
        Ok(Box::new(FakeCall {
            terminal: Some(self.terminals.remove(0)),
        }))
    }
}

struct FakeCall {
    terminal: Option<TransactionEndPortTerminal>,
}

impl TransactionEndPortCall for FakeCall {
    fn poll(&mut self, _deadline_elapsed: bool) -> TransactionEndPortCallPoll {
        self.terminal
            .take()
            .map_or(TransactionEndPortCallPoll::Pending, |terminal| {
                TransactionEndPortCallPoll::Terminal(Box::new(FakeEvidence(terminal)))
            })
    }

    fn discard_after_driver_shutdown(self: Box<Self>) {
        drop(self);
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
