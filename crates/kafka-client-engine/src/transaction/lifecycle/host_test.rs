//! Private transaction lifecycle execution and completion scenarios.

use kafka_client_core::{
    DeliveryStatus, Moment, ProducerRetryPolicy, TransactionEndBrokerFailureKind,
    TransactionEndFailure, TransactionEndFailureKind, TransactionEndMode,
    TransactionLifecycleTerminal,
};

use super::{
    host::{TransactionLifecycleHost, TransactionLifecycleTurn},
    host_support_test::{
        FakePort, RecordedRequest, assert_released, deadline, host, host_with_policy,
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

#[test]
fn deadline_during_end_retry_preserves_possible_delivery() {
    let (mut host, mut port, observer, _completion) = retrying_commit(FakePort::retrying_once());

    schedule_first_retry(&mut host, &mut port);
    assert_eq!(
        host.turn_with_at(Moment::from_tick(31), &mut port),
        Ok(TransactionLifecycleTurn::Progress)
    );
    publish_terminal(&mut host, &mut port, 31);

    assert_uncertain_failure(observer.wait(), TransactionEndFailureKind::DeadlineElapsed);
}

#[test]
fn replacement_rejection_preserves_possible_delivery() {
    let (mut host, mut port, observer, _completion) =
        retrying_commit(FakePort::retrying_then_rejecting());

    schedule_first_retry(&mut host, &mut port);
    assert_eq!(
        host.turn_with_at(Moment::from_tick(1), &mut port),
        Ok(TransactionLifecycleTurn::Progress)
    );
    publish_terminal(&mut host, &mut port, 1);

    assert_uncertain_failure(observer.wait(), TransactionEndFailureKind::DriverRejected);
}

#[test]
fn shutdown_during_end_retry_preserves_possible_delivery() {
    let (mut host, mut port, observer, _completion) = retrying_commit(FakePort::retrying_once());

    schedule_first_retry(&mut host, &mut port);
    host.recover_end_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("shutdown recovery: {error:?}"));
    publish_terminal(&mut host, &mut port, 0);

    assert_uncertain_failure(observer.wait(), TransactionEndFailureKind::DriverClosed);
}

#[test]
fn failed_end_preserves_exact_intent_cause_delivery_and_signed_code() {
    let (mut host, _active, _release, _completion) = host();
    let epoch = host
        .begin()
        .unwrap_or_else(|error| panic!("transaction begins: {error:?}"));
    let deadline = deadline(31);
    let observer = host
        .abort(epoch, deadline)
        .unwrap_or_else(|error| panic!("abort is admitted: {error:?}"));
    let code =
        core::num::NonZeroI16::new(-731).unwrap_or_else(|| panic!("signed test code is nonzero"));
    let failure = TransactionEndFailure::broker(
        TransactionEndMode::Abort,
        TransactionEndBrokerFailureKind::Rejected,
        DeliveryStatus::PossiblySent,
        code,
    );
    let mut port = FakePort::failed(failure);

    drive_three(&mut host, &mut port);
    assert_eq!(
        observer.wait(),
        Ok(TransactionLifecycleTerminal::Failed(failure))
    );
    assert_eq!(
        host.machine.state(),
        kafka_client_core::TransactionLifecycleState::Fatal
    );
}

fn drive_three(host: &mut TransactionLifecycleHost, port: &mut FakePort) {
    for _ in 0..3 {
        host.turn_with(port)
            .unwrap_or_else(|error| panic!("host turn: {error:?}"));
    }
}

fn retrying_commit(
    port: FakePort,
) -> (
    TransactionLifecycleHost,
    FakePort,
    crate::completion::CompletionObserver<TransactionLifecycleTerminal>,
    crate::transaction::completion::TransactionCompletionOwner,
) {
    let retry_policy = ProducerRetryPolicy::try_fixed(1, 1)
        .unwrap_or_else(|_| panic!("one bounded retry with positive backoff"));
    let (mut host, _active, _release, completion) = host_with_policy(retry_policy);
    let epoch = host
        .begin()
        .unwrap_or_else(|error| panic!("transaction begins: {error:?}"));
    let observer = host
        .commit(epoch, deadline(31))
        .unwrap_or_else(|error| panic!("commit is admitted: {error:?}"));
    (host, port, observer, completion)
}

fn schedule_first_retry(host: &mut TransactionLifecycleHost, port: &mut FakePort) {
    for _ in 0..2 {
        assert_eq!(
            host.turn_with_at(Moment::from_tick(0), port),
            Ok(TransactionLifecycleTurn::Progress)
        );
    }
}

fn publish_terminal(host: &mut TransactionLifecycleHost, port: &mut FakePort, tick: u64) {
    assert_eq!(
        host.turn_with_at(Moment::from_tick(tick), port),
        Ok(TransactionLifecycleTurn::Progress)
    );
}

fn assert_uncertain_failure(
    terminal: Result<TransactionLifecycleTerminal, crate::completion::CompletionObserverError>,
    expected_kind: TransactionEndFailureKind,
) {
    let TransactionLifecycleTerminal::Failed(failure) =
        terminal.unwrap_or_else(|error| panic!("terminal wait: {error:?}"))
    else {
        panic!("end must fail");
    };
    assert_eq!(failure.mode(), TransactionEndMode::Commit);
    assert_eq!(failure.kind(), expected_kind);
    assert_eq!(failure.delivery(), DeliveryStatus::PossiblySent);
    assert_eq!(failure.broker_code(), None);
}
