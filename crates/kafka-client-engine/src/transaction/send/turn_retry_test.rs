//! Route refresh, original-deadline backoff, and exact replacement tests.

use std::{num::NonZeroI16, time::Duration};

use kafka_client_core::{
    CompressionPolicy, DeliveryStatus, Moment, ProducerBrokerFailure, ProducerBrokerFailureKind,
    ProducerRetryPolicy,
};

use crate::driver::transaction_produce::{
    TransactionProduceFailureKind, TransactionProduceRouteRefreshPoll,
    TransactionProduceTerminalFact,
};

use super::{
    TransactionSendTerminal,
    test_support::{FakeAggregate, FakeProducePort, driver, produce_failure, request},
};

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one workflow verifies the complete route-refresh and replacement timeline"
)]
fn core_authorized_route_replacement_reuses_exact_bytes_and_original_deadline_without_spin() {
    let policy = ProducerRetryPolicy::try_fixed(2, 10)
        .unwrap_or_else(|error| panic!("bounded retry policy: {error:?}"));
    let mut aggregate = FakeAggregate::with_retry_policy(policy);
    let epoch = aggregate.epoch;
    let mut owner = aggregate.send_owner(CompressionPolicy::None);
    let first_request = request(epoch, "orders", 1_024);
    let original_deadline = first_request.deadline();
    let accepted = owner
        .try_send_with(&mut aggregate, first_request)
        .unwrap_or_else(|error| panic!("send is accepted: {error:?}"));
    let send_id = accepted.send_id();
    let observer = accepted.into_observer();
    aggregate.enrolled();
    let mut driver = driver();
    let mut port = FakeProducePort::success(&aggregate, send_id);
    port.fact = Some(routing_failure(epoch, send_id));
    port.replacement_fact = Some(TransactionProduceTerminalFact::Succeeded {
        epoch,
        send_id,
        success: kafka_client_core::ProducerBatchSuccess::new(42, None, None),
    });
    port.route_refresh_polls
        .lock()
        .unwrap_or_else(|error| panic!("route poll script: {error:?}"))
        .extend([
            TransactionProduceRouteRefreshPoll::Submitted,
            TransactionProduceRouteRefreshPoll::Pending,
            TransactionProduceRouteRefreshPoll::Ready,
        ]);

    for tick in 1..=5 {
        owner
            .turn_with(&mut aggregate, Moment::from_tick(tick), &driver, &mut port)
            .unwrap_or_else(|error| panic!("initial execution turn: {error:?}"));
    }
    assert_eq!(port.submit_count, 1);
    assert_eq!(
        owner.next_deadline(),
        Some(original_deadline.core()),
        "active invalidation remains bounded only by the original public deadline"
    );
    assert_eq!(
        owner
            .turn_with(&mut aggregate, Moment::from_tick(6), &driver, &mut port)
            .unwrap_or_else(|error| panic!("invalidation submission: {error:?}")),
        super::TransactionSendTurn::Progress
    );
    assert_eq!(
        owner
            .turn_with(&mut aggregate, Moment::from_tick(7), &driver, &mut port)
            .unwrap_or_else(|error| panic!("active invalidation: {error:?}")),
        super::TransactionSendTurn::Idle
    );
    assert_eq!(owner.next_deadline(), Some(original_deadline.core()));
    owner
        .turn_with(&mut aggregate, Moment::from_tick(8), &driver, &mut port)
        .unwrap_or_else(|error| panic!("new leader evidence: {error:?}"));
    assert_eq!(
        owner.next_deadline(),
        Some(kafka_client_core::Deadline::from_tick(15))
    );
    assert_eq!(
        owner
            .turn_with(&mut aggregate, Moment::from_tick(14), &driver, &mut port)
            .unwrap_or_else(|error| panic!("backoff is not due: {error:?}")),
        super::TransactionSendTurn::Idle
    );
    assert_eq!(port.submit_count, 1);
    owner
        .turn_with(&mut aggregate, Moment::from_tick(15), &driver, &mut port)
        .unwrap_or_else(|error| panic!("replacement is due: {error:?}"));
    assert_eq!(port.submit_count, 2);
    assert_eq!(
        port.observed_attempts
            .iter()
            .map(|attempt| attempt.get())
            .collect::<Vec<_>>(),
        [0, 1]
    );
    assert_eq!(
        port.observed_deadlines,
        [original_deadline, original_deadline]
    );
    assert_eq!(
        port.observed_transactional_ids,
        ["writer".to_owned(), "writer".to_owned()]
    );
    assert_eq!(port.observed_records.len(), 2);
    assert_eq!(
        port.observed_records[0].as_ptr(),
        port.observed_records[1].as_ptr(),
        "replacement request shares the exact materialized RecordBatch allocation"
    );

    for tick in 16..=18 {
        owner
            .turn_with(&mut aggregate, Moment::from_tick(tick), &driver, &mut port)
            .unwrap_or_else(|error| panic!("replacement settlement: {error:?}"));
    }
    assert!(matches!(
        observer.wait(),
        Ok(TransactionSendTerminal::Succeeded {
            epoch: terminal_epoch,
            send_id: terminal_send,
            ..
        }) if terminal_epoch == epoch && terminal_send == send_id
    ));
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("driver shuts down: {error:?}"));
}

#[test]
fn pending_invalidation_settles_at_original_deadline_without_retry_spin() {
    let policy = ProducerRetryPolicy::try_fixed(2, 10)
        .unwrap_or_else(|error| panic!("bounded retry policy: {error:?}"));
    let mut aggregate = FakeAggregate::with_retry_policy(policy);
    let epoch = aggregate.epoch;
    let mut owner = aggregate.send_owner(CompressionPolicy::None);
    let accepted = owner
        .try_send_with(&mut aggregate, request(epoch, "orders", 1_024))
        .unwrap_or_else(|error| panic!("send accepted: {error:?}"));
    let send_id = accepted.send_id();
    let observer = accepted.into_observer();
    aggregate.enrolled();
    let mut driver = driver();
    let mut port = FakeProducePort::success(&aggregate, send_id);
    port.fact = Some(routing_failure(epoch, send_id));
    port.route_refresh_polls
        .lock()
        .unwrap_or_else(|error| panic!("route poll script: {error:?}"))
        .push_back(TransactionProduceRouteRefreshPoll::Pending);

    for tick in 1..=5 {
        owner
            .turn_with(&mut aggregate, Moment::from_tick(tick), &driver, &mut port)
            .unwrap_or_else(|error| panic!("initial execution: {error:?}"));
    }
    assert_eq!(
        owner
            .turn_with(&mut aggregate, Moment::from_tick(6), &driver, &mut port)
            .unwrap_or_else(|error| panic!("invalidation remains pending: {error:?}")),
        super::TransactionSendTurn::Idle
    );
    assert_eq!(
        owner.next_deadline(),
        Some(kafka_client_core::Deadline::from_tick(50))
    );
    assert_eq!(
        owner
            .turn_with(&mut aggregate, Moment::from_tick(50), &driver, &mut port)
            .unwrap_or_else(|error| panic!("original deadline settles retained fact: {error:?}")),
        super::TransactionSendTurn::Progress
    );
    owner
        .turn_with(&mut aggregate, Moment::from_tick(51), &driver, &mut port)
        .unwrap_or_else(|error| panic!("publish deadline terminal: {error:?}"));
    assert_eq!(port.submit_count, 1);
    assert!(matches!(
        observer.wait(),
        Ok(TransactionSendTerminal::AbortRequired { failure, .. })
            if failure.kind() == super::TransactionSendFailureKind::DeadlineElapsed
                && failure.delivery() == DeliveryStatus::PossiblySent
    ));
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("driver shuts down: {error:?}"));
}

fn routing_failure(
    epoch: kafka_client_core::TransactionEpoch,
    send_id: kafka_client_core::TransactionSendId,
) -> TransactionProduceTerminalFact {
    TransactionProduceTerminalFact::AbortRequired {
        epoch,
        send_id,
        failure: produce_failure(
            TransactionProduceFailureKind::Broker(ProducerBrokerFailure::new(
                ProducerBrokerFailureKind::Routing,
                NonZeroI16::new(6).unwrap_or_else(|| panic!("nonzero broker code")),
            )),
            DeliveryStatus::PossiblySent,
        ),
    }
}
