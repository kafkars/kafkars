//! Tracked transaction-partition call and terminal scenarios.

use kafka_client_core::DeliveryStatus;
use kafka_driver::{ApiVersion, CallFailure, Delivery, RequestError, RouteKind};
use kafka_wire::{
    AddPartitionsToTxnResponse,
    add_partitions_to_txn_response::{
        AddPartitionsToTxnPartitionResult, AddPartitionsToTxnTopicResult,
    },
};

use crate::protocol::transaction::{
    AddPartitionsToTxnPartitionOutcome, AddPartitionsToTxnResponseFailure,
};

use super::{
    TransactionAddPartitionsTerminalFact, TransactionControlDriverFailureKind,
    TransactionPartitionTarget, add_partitions::TransactionAddPartitionsTerminal,
};

#[test]
fn v3_terminal_normalizes_success_and_malformed_partition_shape() {
    let terminal = TransactionAddPartitionsTerminal::new(
        Some(ApiVersion::new(3)),
        Ok(response(2)),
        None,
        targets(),
    );
    let TransactionAddPartitionsTerminalFact::Response(Ok(normalized)) = terminal.fact() else {
        panic!("valid v3 response expected");
    };
    assert!(matches!(
        normalized.partitions()[0].outcome(),
        AddPartitionsToTxnPartitionOutcome::Added
    ));
    terminal.discard();

    let malformed = TransactionAddPartitionsTerminal::new(
        Some(ApiVersion::new(3)),
        Ok(response(99)),
        None,
        targets(),
    );
    assert!(matches!(
        malformed.fact(),
        TransactionAddPartitionsTerminalFact::Response(Err(
            AddPartitionsToTxnResponseFailure::MissingPartition { actual: 2 }
        ))
    ));
    malformed.discard();
}

#[test]
fn missing_or_wrong_selected_version_is_not_interpreted_as_v3() {
    for (version, expected) in [
        (None, TransactionControlDriverFailureKind::InvalidResponse),
        (
            Some(ApiVersion::new(2)),
            TransactionControlDriverFailureKind::Compatibility,
        ),
    ] {
        let terminal =
            TransactionAddPartitionsTerminal::new(version, Ok(response(2)), None, targets());
        assert!(matches!(
            terminal.fact(),
            TransactionAddPartitionsTerminalFact::Failed {
                kind,
                delivery: DeliveryStatus::PossiblySent,
            } if kind == expected
        ));
        terminal.discard();
    }
}

#[test]
fn only_coordinator_route_failures_and_broker_codes_14_through_16_request_refresh() {
    for code in [14, 15, 16] {
        let terminal = broker_terminal(code);
        assert!(terminal.should_refresh_route(RouteKind::Coordinator));
        assert!(!terminal.should_refresh_route(RouteKind::PartitionLeader));
        terminal.discard();
    }
    for code in [0, 13, 17, 25, 47] {
        let terminal = broker_terminal(code);
        assert!(!terminal.should_refresh_route(RouteKind::Coordinator));
        terminal.discard();
    }

    let terminal = driver_terminal(RequestError::RouteUnavailable);
    assert!(terminal.should_refresh_route(RouteKind::Coordinator));
    assert!(!terminal.should_refresh_route(RouteKind::Controller));
    terminal.discard();

    let mut tokenless_deadline = driver_terminal(RequestError::Rejected {
        failure: CallFailure::DeadlineExceeded,
        delivery: Delivery::NotSent,
    });
    assert!(tokenless_deadline.should_refresh_route(RouteKind::Coordinator));
    assert!(
        tokenless_deadline
            .take_transaction_coordinator_refresh_token()
            .is_none()
    );
    assert!(!tokenless_deadline.coordinator_refresh_completed());
    tokenless_deadline.discard();
}

#[test]
fn retry_safe_marker_requires_crossed_barrier_and_authoritative_certainty() {
    for code in [14, 15, 16] {
        let mut terminal = broker_terminal(code);
        assert!(!terminal.retry_safe_after_refresh());
        terminal.mark_coordinator_refresh_completed();
        assert!(terminal.coordinator_refresh_completed());
        assert!(terminal.retry_safe_after_refresh());
        terminal.discard();
    }

    let mut not_sent = driver_terminal(RequestError::RouteUnavailable);
    not_sent.mark_coordinator_refresh_completed();
    assert!(not_sent.retry_safe_after_refresh());
    not_sent.discard();

    let mut deadline_not_sent = driver_terminal(RequestError::Rejected {
        failure: CallFailure::DeadlineExceeded,
        delivery: Delivery::NotSent,
    });
    deadline_not_sent.mark_coordinator_refresh_completed();
    assert!(deadline_not_sent.retry_safe_after_refresh());
    deadline_not_sent.discard();

    let mut possibly_sent = driver_terminal(RequestError::Rejected {
        failure: CallFailure::NotReady,
        delivery: Delivery::PossiblySent,
    });
    possibly_sent.mark_coordinator_refresh_completed();
    assert!(possibly_sent.coordinator_refresh_completed());
    assert!(!possibly_sent.retry_safe_after_refresh());
    possibly_sent.discard();
}

fn targets() -> Vec<TransactionPartitionTarget> {
    vec![TransactionPartitionTarget::new("orders".into(), 2)]
}

fn broker_terminal(error_code: i16) -> TransactionAddPartitionsTerminal {
    TransactionAddPartitionsTerminal::new(
        Some(ApiVersion::new(3)),
        Ok(response_with_error(2, error_code)),
        None,
        targets(),
    )
}

fn driver_terminal(error: RequestError) -> TransactionAddPartitionsTerminal {
    TransactionAddPartitionsTerminal::new(Some(ApiVersion::new(3)), Err(error), None, targets())
}

fn response(partition_index: i32) -> AddPartitionsToTxnResponse {
    response_with_error(partition_index, 0)
}

fn response_with_error(partition_index: i32, error_code: i16) -> AddPartitionsToTxnResponse {
    let mut partition = AddPartitionsToTxnPartitionResult::default();
    partition.partition_index = partition_index;
    partition.partition_error_code = error_code;
    let mut topic = AddPartitionsToTxnTopicResult::default();
    topic.name = "orders".into();
    topic.results_by_partition = vec![partition];
    let mut response = AddPartitionsToTxnResponse::default();
    response.throttle_time_ms = 17;
    response.results_by_topic_v3_and_below = vec![topic];
    response
}
