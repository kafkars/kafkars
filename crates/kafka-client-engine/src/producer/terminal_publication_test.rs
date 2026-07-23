//! Host terminal publication, independent mechanism progress, and recovery scenarios.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use kafka_client_core::{
    Deadline, DeliveryStatus, FlushId, Moment, OperationId, ProducerCompletion, ProducerEffect,
    ProducerFailureKind,
};

use crate::{
    ProducerDeliveryError, ProducerDeliveryFailureKind, ProducerDeliveryStatus,
    completion::CompletionRegistryError,
};

use super::{
    ProducerHost,
    admission::AdmittedExplicit,
    admission_test::{admit, record},
    host_limits_test::{start, valid_limits},
    terminal_backlog::{ProducerTerminalOwner, RetainedTerminal},
};

#[test]
fn flush_publication_stays_behind_retained_record_terminal_under_notifier_backpressure() {
    let mut host = start(valid_limits());
    let record = admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(5),
        record("orders"),
    );
    let flush = host
        .try_admit_flush(Moment::from_tick(1))
        .unwrap_or_else(|error| panic!("flush should be accepted: {error:?}"));
    host.inject_terminal_publish_fault(CompletionRegistryError::NotificationBackpressure);

    assert_eq!(host.fire_due(Moment::from_tick(5), 1), Ok(1));
    assert_eq!(host.stats().terminal_backlog, 2);
    assert_eq!(
        host.terminal_front().map(RetainedTerminal::owner),
        Some(ProducerTerminalOwner::Record(record.operation_id()))
    );
    assert_eq!(
        host.terminal_back().map(RetainedTerminal::owner),
        Some(ProducerTerminalOwner::Flush(flush.flush_id()))
    );

    assert_eq!(host.retry_terminal_backlog(1), Ok(1));
    assert_deadline_failure(record);
    let mut flush = flush.into_flush_observer();
    let waker = Waker::noop();
    assert_eq!(
        Pin::new(&mut flush).poll(&mut Context::from_waker(waker)),
        Poll::Pending
    );
    assert_eq!(host.stats().terminal_backlog, 1);
    assert_eq!(
        host.terminal_front().map(RetainedTerminal::owner),
        Some(ProducerTerminalOwner::Flush(FlushId::from_raw(1)))
    );

    assert_eq!(host.retry_terminal_backlog(1), Ok(1));
    assert_eq!(flush.wait(), Ok(()));
}

#[test]
fn close_publication_stays_behind_prior_record_under_notifier_backpressure() {
    let mut host = start(valid_limits());
    let record = admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(5),
        record("orders"),
    );
    let close = host
        .try_admit_close(Moment::from_tick(1))
        .unwrap_or_else(|error| panic!("close should be accepted: {error:?}"));
    host.inject_terminal_publish_fault(CompletionRegistryError::NotificationBackpressure);

    assert_eq!(host.fire_due(Moment::from_tick(5), 1), Ok(1));
    assert_eq!(
        host.terminal_front().map(RetainedTerminal::owner),
        Some(ProducerTerminalOwner::Record(record.operation_id()))
    );
    assert_eq!(
        host.terminal_back().map(RetainedTerminal::owner),
        Some(ProducerTerminalOwner::Flush(close.flush_id()))
    );

    assert_eq!(host.retry_terminal_backlog(1), Ok(1));
    assert_deadline_failure(record);
    let mut close = close.into_flush_observer();
    let waker = Waker::noop();
    assert_eq!(
        Pin::new(&mut close).poll(&mut Context::from_waker(waker)),
        Poll::Pending
    );
    assert_eq!(host.retry_terminal_backlog(1), Ok(1));
    assert_eq!(close.wait(), Ok(()));
}

#[test]
fn notification_backpressure_retains_the_exact_terminal_fifo() {
    let (mut host, operation, admitted) = backlogged_deadline();

    assert_eq!(host.stats().terminal_backlog, 1);
    assert_eq!(host.stats().pending_effects, 0);
    assert_terminal_capacity_bound(&host);
    assert_deadline_terminal(host.terminal_front(), operation);
    assert_eq!(host.retry_terminal_backlog(1), Ok(1));
    assert_eq!(host.stats().terminal_backlog, 0);
    assert_terminal_capacity_bound(&host);
    assert_deadline_failure(admitted);
}

#[test]
fn later_record_terminals_append_behind_a_blocked_record() {
    let mut host = start(valid_limits());
    let first = admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(5),
        record("orders"),
    );
    let second = admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(5),
        record("payments"),
    );
    host.inject_terminal_publish_fault(CompletionRegistryError::NotificationBackpressure);

    assert_eq!(host.fire_due(Moment::from_tick(5), 2), Ok(2));
    assert_eq!(host.terminal_publish_attempts(), 1);
    assert_eq!(host.stats().terminal_backlog, 2);
    assert_terminal_capacity_bound(&host);
    assert_deadline_terminal(host.terminal_front(), first.operation_id());
    assert_deadline_terminal(host.terminal_back(), second.operation_id());

    assert_eq!(host.retry_terminal_backlog(2), Ok(2));
    assert_terminal_capacity_bound(&host);
    assert_deadline_failure(first);
    assert_deadline_failure(second);
}

#[test]
fn retry_pops_only_after_notifier_acceptance() {
    let (mut host, operation, admitted) = backlogged_deadline();
    host.inject_terminal_publish_fault(CompletionRegistryError::NotificationBackpressure);

    assert_eq!(host.retry_terminal_backlog(1), Ok(0));
    assert_eq!(host.stats().terminal_backlog, 1);
    assert_deadline_terminal(host.terminal_front(), operation);
    assert_eq!(host.retry_terminal_backlog(1), Ok(1));
    assert_eq!(host.stats().terminal_backlog, 0);
    assert_deadline_failure(admitted);
}

#[test]
fn materialization_and_unrelated_admission_continue_while_terminals_are_backlogged() {
    let (mut host, _operation, first) = backlogged_deadline();
    let second = admit(
        &mut host,
        Moment::from_tick(10),
        Deadline::from_tick(200),
        record("payments"),
    );

    assert_eq!(host.fire_due(Moment::from_tick(110), 1), Ok(1));
    assert_eq!(host.drive_prepared(Moment::from_tick(111), 1), Ok(1));
    assert_eq!(host.stats().terminal_backlog, 1);
    assert_eq!(host.stats().prepared_batches, 1);
    assert!(matches!(
        host.pending_effects(),
        [ProducerEffect::SubmitProduce { .. }]
    ));

    assert_eq!(host.retry_terminal_backlog(1), Ok(1));
    assert_deadline_failure(first);
    drop(second);
}

fn backlogged_deadline() -> (ProducerHost, OperationId, AdmittedExplicit) {
    let mut host = start(valid_limits());
    let admitted = admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(5),
        record("orders"),
    );
    let operation = admitted.operation_id();
    host.inject_terminal_publish_fault(CompletionRegistryError::NotificationBackpressure);
    assert_eq!(host.fire_due(Moment::from_tick(5), 1), Ok(1));
    (host, operation, admitted)
}

fn assert_deadline_terminal(terminal: Option<&RetainedTerminal>, expected: OperationId) {
    let Some(terminal) = terminal else {
        panic!("backlog must retain one exact failed record terminal")
    };
    let Some(ProducerCompletion::Failed(failure)) = terminal.record_completion() else {
        panic!("deadline should retain its exact failure")
    };
    assert_eq!(terminal.owner(), ProducerTerminalOwner::Record(expected));
    assert_eq!(failure.kind(), ProducerFailureKind::DeadlineElapsed);
    assert_eq!(failure.delivery(), DeliveryStatus::NotSent);
}

fn assert_deadline_failure(admitted: AdmittedExplicit) {
    let result = admitted.into_delivery_observer().wait();
    let Err(ProducerDeliveryError::Failed(failure)) = result else {
        panic!("deadline terminal should remain observable")
    };
    assert_eq!(failure.kind(), ProducerDeliveryFailureKind::DeadlineElapsed);
    assert_eq!(failure.delivery_status(), ProducerDeliveryStatus::NotSent);
}

fn assert_terminal_capacity_bound(host: &ProducerHost) {
    let occupied = host
        .terminal_backlog
        .len()
        .saturating_add(host.completions.published_or_reclaiming_len());
    assert!(occupied <= host.effect_capacity);
}
