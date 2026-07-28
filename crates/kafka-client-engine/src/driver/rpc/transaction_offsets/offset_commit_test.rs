//! Tracked `TxnOffsetCommit` v4 call and terminal scenarios.

use std::sync::Arc;
use std::time::{Duration, Instant};

use kafka_client_core::DeliveryStatus;
use kafka_driver::{ApiVersion, CallFailure, Delivery, RequestError, RouteKind};
use kafka_wire::{
    TxnOffsetCommitResponse,
    txn_offset_commit_response::{TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic},
};

use crate::{
    EngineConfig,
    protocol::transaction::{
        TransactionBrokerCategory, TransactionGroupIdentityRef, TransactionOffsetCommitOutcome,
        TxnOffsetCommitRequestFailure, TxnOffsetCommitResponseFailure,
    },
};

use super::super::super::DriverOwner;
use super::{
    TransactionOffsetCommitCall, TransactionOffsetCommitCallAdmissionFailure,
    TransactionOffsetCommitTarget, TransactionOffsetCommitTerminalFact,
    TransactionOffsetDriverFailureKind, offset_commit::TransactionOffsetCommitTerminal,
};

#[test]
fn empty_offsets_are_definitely_unsent() {
    let driver = test_driver();
    assert!(matches!(
        TransactionOffsetCommitCall::submit(
            &driver,
            "writer",
            42,
            7,
            group(),
            Vec::new(),
            Instant::now() + Duration::from_secs(1),
        ),
        Err(TransactionOffsetCommitCallAdmissionFailure::Request(
            TxnOffsetCommitRequestFailure::EmptyOffsets
        ))
    ));
}

#[test]
fn v4_terminal_restores_caller_order_and_preserves_signed_errors() {
    let terminal = TransactionOffsetCommitTerminal::new(
        Some(ApiVersion::new(4)),
        Ok(valid_response()),
        None,
        targets(),
    );
    let TransactionOffsetCommitTerminalFact::Response(Ok(normalized)) = terminal.fact() else {
        panic!("valid v4 response expected");
    };
    assert_eq!(normalized.throttle_time_ms(), 19);
    let results = normalized.offsets();
    assert_eq!(results[0].offset().topic(), "orders");
    assert_eq!(results[0].offset().partition(), 2);
    assert!(matches!(
        results[0].outcome(),
        TransactionOffsetCommitOutcome::Committed
    ));
    let TransactionOffsetCommitOutcome::Rejected(error) = results[1].outcome() else {
        panic!("signed error must remain a rejection");
    };
    assert_eq!(results[1].offset().topic(), "audit");
    assert_eq!(error.code().get(), -31_000);
    assert_eq!(error.category(), TransactionBrokerCategory::Rejected);
    terminal.discard();
}

#[test]
fn malformed_correlation_and_wrong_versions_are_terminal_failures() {
    let malformed = TransactionOffsetCommitTerminal::new(
        Some(ApiVersion::new(4)),
        Ok(response(
            0,
            vec![
                topic("orders", vec![partition(2, 0), partition(99, 0)]),
                topic("audit", vec![partition(1, 0)]),
            ],
        )),
        None,
        targets(),
    );
    assert!(matches!(
        malformed.fact(),
        TransactionOffsetCommitTerminalFact::Response(Err(
            TxnOffsetCommitResponseFailure::MissingPartition { actual: 7 }
        ))
    ));
    malformed.discard();

    for (version, expected) in [
        (None, TransactionOffsetDriverFailureKind::InvalidResponse),
        (
            Some(ApiVersion::new(3)),
            TransactionOffsetDriverFailureKind::Compatibility,
        ),
    ] {
        let terminal =
            TransactionOffsetCommitTerminal::new(version, Ok(valid_response()), None, targets());
        assert!(matches!(
            terminal.fact(),
            TransactionOffsetCommitTerminalFact::Failed {
                kind,
                delivery: DeliveryStatus::PossiblySent,
            } if kind == expected
        ));
        terminal.discard();
    }
}

#[test]
fn only_exact_group_coordinator_recovery_evidence_requests_refresh() {
    for code in [14, 15, 16] {
        let terminal = broker_terminal(code);
        assert_only_group_coordinator_refresh(&terminal);
        terminal.discard();
    }
    for code in [0, 22, 25, 27, 47] {
        let terminal = broker_terminal(code);
        assert!(!terminal.should_refresh_route(RouteKind::Coordinator));
        terminal.discard();
    }

    for error in [
        RequestError::RouteUnavailable,
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            delivery: Delivery::PossiblySent,
        },
    ] {
        let terminal = TransactionOffsetCommitTerminal::new(
            Some(ApiVersion::new(4)),
            Err(error),
            None,
            targets(),
        );
        assert_only_group_coordinator_refresh(&terminal);
        terminal.discard();
    }
}

fn assert_only_group_coordinator_refresh(terminal: &TransactionOffsetCommitTerminal) {
    assert!(
        terminal.should_refresh_route(RouteKind::Coordinator)
            && !terminal.should_refresh_route(RouteKind::Controller)
            && !terminal.should_refresh_route(RouteKind::PartitionLeader)
    );
}

#[test]
fn compatibility_and_malformed_responses_do_not_invalidate_the_group_route() {
    let wrong_version = TransactionOffsetCommitTerminal::new(
        Some(ApiVersion::new(3)),
        Ok(valid_response()),
        None,
        targets(),
    );
    assert!(!wrong_version.should_refresh_route(RouteKind::Coordinator));
    wrong_version.discard();

    let malformed = TransactionOffsetCommitTerminal::new(
        Some(ApiVersion::new(4)),
        Ok(response(0, vec![topic("orders", vec![partition(99, 15)])])),
        None,
        targets(),
    );
    assert!(!malformed.should_refresh_route(RouteKind::Coordinator));
    malformed.discard();
}

#[test]
fn only_exact_refreshed_group_coordinator_rejection_is_retry_safe() {
    for code in [14, 15, 16] {
        let mut terminal = broker_terminal(code);
        assert!(!terminal.retry_safe_after_refresh());
        terminal.mark_coordinator_refresh_completed();
        assert!(terminal.retry_safe_after_refresh());
        terminal.discard();
    }
    for code in [0, 22, 25, 27, 47, -731] {
        let mut terminal = broker_terminal(code);
        terminal.mark_coordinator_refresh_completed();
        assert!(!terminal.retry_safe_after_refresh());
        terminal.discard();
    }
    let mut mixed = TransactionOffsetCommitTerminal::new(
        Some(ApiVersion::new(4)),
        Ok(response(
            0,
            vec![
                topic("orders", vec![partition(2, 16), partition(7, 47)]),
                topic("audit", vec![partition(1, 0)]),
            ],
        )),
        None,
        targets(),
    );
    mixed.mark_coordinator_refresh_completed();
    assert!(!mixed.retry_safe_after_refresh());
    mixed.discard();

    let mut ambiguous = TransactionOffsetCommitTerminal::new(
        Some(ApiVersion::new(4)),
        Err(RequestError::RouteUnavailable),
        None,
        targets(),
    );
    ambiguous.mark_coordinator_refresh_completed();
    assert!(!ambiguous.retry_safe_after_refresh());
    ambiguous.discard();
}

fn test_driver() -> DriverOwner {
    DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"))
}

fn group() -> TransactionGroupIdentityRef<'static> {
    TransactionGroupIdentityRef::new("workers", 5, "member-a", Some("instance-a"))
}

fn targets() -> Vec<TransactionOffsetCommitTarget> {
    vec![
        TransactionOffsetCommitTarget::new(
            Arc::from("orders"),
            2,
            93,
            Some(7),
            Some(Arc::from("checkpoint-a")),
        ),
        TransactionOffsetCommitTarget::new(Arc::from("audit"), 1, 12, None, None),
        TransactionOffsetCommitTarget::new(
            Arc::from("orders"),
            7,
            120,
            Some(9),
            Some(Arc::from("")),
        ),
    ]
}

fn valid_response() -> TxnOffsetCommitResponse {
    response(
        19,
        vec![
            topic("audit", vec![partition(1, -31_000)]),
            topic("orders", vec![partition(7, 47), partition(2, 0)]),
        ],
    )
}

fn broker_terminal(error_code: i16) -> TransactionOffsetCommitTerminal {
    TransactionOffsetCommitTerminal::new(
        Some(ApiVersion::new(4)),
        Ok(response(
            0,
            vec![
                topic("orders", vec![partition(2, error_code), partition(7, 0)]),
                topic("audit", vec![partition(1, 0)]),
            ],
        )),
        None,
        targets(),
    )
}

fn response(
    throttle_time_ms: i32,
    topics: Vec<TxnOffsetCommitResponseTopic>,
) -> TxnOffsetCommitResponse {
    let mut response = TxnOffsetCommitResponse::default();
    response.throttle_time_ms = throttle_time_ms;
    response.topics = topics;
    response
}

fn topic(
    name: &str,
    partitions: Vec<TxnOffsetCommitResponsePartition>,
) -> TxnOffsetCommitResponseTopic {
    let mut topic = TxnOffsetCommitResponseTopic::default();
    topic.name = name.into();
    topic.partitions = partitions;
    topic
}

fn partition(partition_index: i32, error_code: i16) -> TxnOffsetCommitResponsePartition {
    let mut partition = TxnOffsetCommitResponsePartition::default();
    partition.partition_index = partition_index;
    partition.error_code = error_code;
    partition
}
