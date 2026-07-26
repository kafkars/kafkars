//! Lossless failure scenarios for membership route-token transfer.

use kafka_wire::{HeartbeatResponse, JoinGroupResponse, SyncGroupResponse};

use super::{
    heartbeat_calls::{AcceptedClassicHeartbeatCall, TrackedClassicHeartbeatCalls},
    heartbeat_settlement::{ClassicHeartbeatConfirmationError, ClassicHeartbeatPoll},
    heartbeat_terminal_test::key as heartbeat_key,
    join_group_calls::{AcceptedJoinGroupCall, TrackedJoinGroupCalls},
    join_group_settlement::{JoinGroupConfirmationError, JoinGroupPoll},
    join_group_terminal_test::key as join_key,
    sync_group_calls::{AcceptedSyncGroupCall, TrackedSyncGroupCalls},
    sync_group_settlement::{SyncGroupConfirmationError, SyncGroupPoll},
    sync_group_terminal_test::key as sync_key,
};

#[test]
fn join_without_route_evidence_retains_pending_confirmation_and_receipt() {
    let key = join_key(1);
    let receipt = AcceptedJoinGroupCall::from_key_for_test(key);
    let mut calls = TrackedJoinGroupCalls::new(1);
    calls.install_terminal_for_test(key, Some(3), Ok(JoinGroupResponse::default()));
    let _terminal = calls
        .begin_join_group_settlement(&receipt)
        .unwrap_or_else(|error| panic!("test settlement failed: {error:?}"));

    let failure = calls
        .extract_join_group_rediscovery(receipt)
        .err()
        .unwrap_or_else(|| panic!("missing route evidence must reject transfer"));
    let (receipt, error) = failure.into_parts();

    assert_eq!(receipt.key(), key);
    assert_eq!(
        error,
        JoinGroupConfirmationError::RouteTokenUnavailable { pending: key }
    );
    assert_eq!(
        calls.poll_join_group(),
        Ok(JoinGroupPoll::ConfirmationPending { key })
    );
}

#[test]
fn sync_without_route_evidence_retains_pending_confirmation_and_receipt() {
    let key = sync_key(1);
    let receipt = AcceptedSyncGroupCall::from_key_for_test(key);
    let mut calls = TrackedSyncGroupCalls::new(1);
    calls.install_terminal_for_test(key, Some(2), Ok(SyncGroupResponse::default()));
    let _terminal = calls
        .begin_sync_group_settlement(&receipt)
        .unwrap_or_else(|error| panic!("test settlement failed: {error:?}"));

    let failure = calls
        .extract_sync_group_rediscovery(receipt)
        .err()
        .unwrap_or_else(|| panic!("missing route evidence must reject transfer"));
    let (receipt, error) = failure.into_parts();

    assert_eq!(receipt.key(), key);
    assert_eq!(
        error,
        SyncGroupConfirmationError::RouteTokenUnavailable { pending: key }
    );
    assert_eq!(
        calls.poll_sync_group(),
        Ok(SyncGroupPoll::ConfirmationPending { key })
    );
}

#[test]
fn heartbeat_without_route_evidence_retains_pending_confirmation_and_receipt() {
    let key = heartbeat_key(1);
    let receipt = AcceptedClassicHeartbeatCall::from_key_for_test(key);
    let mut calls = TrackedClassicHeartbeatCalls::new(1);
    calls.install_terminal_for_test(key, Some(2), Ok(HeartbeatResponse::default()));
    let _terminal = calls
        .begin_classic_heartbeat_settlement(&receipt)
        .unwrap_or_else(|error| panic!("test settlement failed: {error:?}"));

    let failure = calls
        .extract_classic_heartbeat_rediscovery(receipt)
        .err()
        .unwrap_or_else(|| panic!("missing route evidence must reject transfer"));
    let (receipt, error) = failure.into_parts();

    assert_eq!(receipt.key(), key);
    assert_eq!(
        error,
        ClassicHeartbeatConfirmationError::RouteTokenUnavailable { pending: key }
    );
    assert_eq!(
        calls.poll_classic_heartbeat(),
        Ok(ClassicHeartbeatPoll::ConfirmationPending { key })
    );
}
