//! Linear `TxnOffsetCommit` call-state and shutdown-recovery scenarios.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use kafka_client_core::DeliveryStatus;
use kafka_driver::{ApiVersion, CompletionError, RequestError};

use crate::{EngineConfig, protocol::transaction::TransactionGroupIdentityRef};

use super::{
    super::super::DriverOwner, TransactionOffsetCommitCall, TransactionOffsetCommitPoll,
    TransactionOffsetCommitTarget, TransactionOffsetCommitTerminal,
    TransactionOffsetCommitTerminalFact, TransactionOffsetDriverFailureKind,
    offset_commit::RecoveredTransactionOffsetCommitCall,
};

#[test]
fn calling_state_completes_once_or_recovers_exact_targets_after_shutdown() {
    let driver = test_driver();
    drop(
        accepted_call(&driver)
            .recover_after_driver_shutdown()
            .unwrap_or_else(|| panic!("accepted call retained")),
    );

    let driver = test_driver();
    let mut call = accepted_call(&driver);
    drop(driver);
    assert!(matches!(
        call.poll(),
        TransactionOffsetCommitPoll::Terminal(Err(CompletionError::Closed))
    ));
    assert!(matches!(call.poll(), TransactionOffsetCommitPoll::Pending));
    assert!(call.recover_after_driver_shutdown().is_none());
}

#[test]
fn refreshing_shutdown_recovery_preserves_exact_offset_commit_terminal() {
    let recovered =
        RecoveredTransactionOffsetCommitCall::terminal(TransactionOffsetCommitTerminal::new(
            Some(ApiVersion::new(4)),
            Err(RequestError::RouteUnavailable),
            None,
            targets(),
        ));
    let terminal = recovered
        .into_terminal()
        .unwrap_or_else(|| panic!("refreshing recovery must retain the known terminal"));
    assert!(matches!(
        terminal.fact(),
        TransactionOffsetCommitTerminalFact::Failed {
            kind: TransactionOffsetDriverFailureKind::Transport,
            delivery: DeliveryStatus::NotSent,
        }
    ));
    terminal.discard();
}

fn test_driver() -> DriverOwner {
    DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"))
}

fn accepted_call(driver: &DriverOwner) -> TransactionOffsetCommitCall {
    TransactionOffsetCommitCall::submit(
        driver,
        "writer",
        42,
        7,
        TransactionGroupIdentityRef::new("workers", 5, "member-a", Some("instance-a")),
        targets(),
        Instant::now() + Duration::from_secs(1),
    )
    .unwrap_or_else(|error| panic!("accepted call: {error}"))
}

fn targets() -> Vec<TransactionOffsetCommitTarget> {
    vec![TransactionOffsetCommitTarget::new(
        Arc::from("orders"),
        2,
        93,
        Some(7),
        Some(Arc::from("checkpoint-a")),
    )]
}
