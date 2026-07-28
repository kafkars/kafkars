//! Consumer-observable position-failure termination scenarios.

use std::{
    marker::PhantomData,
    sync::{Arc, mpsc::sync_channel},
    thread,
    time::Duration,
};

use kafka_client_core::{GroupPositionMissingOffsetPolicy, Moment};

use crate::{
    consumer::{
        GroupConsumerHandle, GroupConsumerPositionFailureKind, GroupConsumerRecvErrorKind,
        GroupConsumerTryTakeBatchErrorKind,
    },
    driver::GroupPositionOffsetFetchDriverFailureKind,
};

use super::{
    classic_group_position::{
        ClassicGroupPositionSettlementTurn,
        settlement_test_support::{
            PartitionValue, PositionSettlementFixture, driver_owned_fixture_with_policy,
            install_driver_failure, install_legacy_terminal,
        },
    },
    classic_group_position_reset::ClassicGroupPositionResetTurn,
    registry_shard::GroupConsumerShardOwner,
};

#[test]
fn default_missing_offset_error_wakes_blocking_recv_with_exact_failure() {
    let mut fixture =
        driver_owned_fixture_with_policy(&[0], GroupPositionMissingOffsetPolicy::Error);
    install_legacy_terminal(&mut fixture, Some(7), 0, 0, &[(0, PartitionValue::Missing)]);
    settle_and_confirm(&mut fixture);

    assert_blocking_recv_observes_position_failure(
        fixture,
        GroupConsumerPositionFailureKind::MissingOffset,
    );
}

#[test]
fn expired_list_offsets_reset_wakes_blocking_recv_with_exact_failure() {
    let mut fixture =
        driver_owned_fixture_with_policy(&[0], GroupPositionMissingOffsetPolicy::Earliest);
    install_legacy_terminal(&mut fixture, Some(7), 0, 0, &[(0, PartitionValue::Missing)]);
    settle_and_confirm(&mut fixture);
    assert_eq!(
        fixture
            .registry
            .begin_one_classic_group_position_reset(Moment::from_tick(u64::MAX)),
        Ok(ClassicGroupPositionResetTurn::Progress)
    );

    assert_blocking_recv_observes_position_failure(
        fixture,
        GroupConsumerPositionFailureKind::DeadlineElapsed,
    );
}

#[test]
fn exact_group_broker_rejection_is_observed_once_before_retained_fault() {
    let mut fixture =
        driver_owned_fixture_with_policy(&[0], GroupPositionMissingOffsetPolicy::Error);
    install_legacy_terminal(&mut fixture, Some(7), 0, -911, &[]);
    settle_and_confirm(&mut fixture);

    assert_immediate_observation_is_one_shot(
        fixture,
        GroupConsumerPositionFailureKind::Broker(-911),
    );
}

#[test]
fn exact_partition_broker_rejection_is_observed_once_before_retained_fault() {
    let mut fixture =
        driver_owned_fixture_with_policy(&[0, 1], GroupPositionMissingOffsetPolicy::Error);
    install_legacy_terminal(
        &mut fixture,
        Some(7),
        0,
        0,
        &[
            (0, PartitionValue::Committed(8)),
            (1, PartitionValue::Rejected(-731)),
        ],
    );
    settle_and_confirm(&mut fixture);

    assert_immediate_observation_is_one_shot(
        fixture,
        GroupConsumerPositionFailureKind::Broker(-731),
    );
}

#[test]
fn transport_failure_is_observed_once_before_retained_fault() {
    let mut fixture =
        driver_owned_fixture_with_policy(&[0], GroupPositionMissingOffsetPolicy::Error);
    install_driver_failure(
        &mut fixture,
        GroupPositionOffsetFetchDriverFailureKind::Transport,
    );
    settle_and_confirm(&mut fixture);

    assert_immediate_observation_is_one_shot(fixture, GroupConsumerPositionFailureKind::Transport);
}

#[test]
fn dropping_an_unpolled_recv_does_not_consume_the_ready_position_failure() {
    let mut fixture =
        driver_owned_fixture_with_policy(&[0], GroupPositionMissingOffsetPolicy::Error);
    install_legacy_terminal(&mut fixture, Some(7), 0, 0, &[(0, PartitionValue::Missing)]);
    settle_and_confirm(&mut fixture);
    let (owner, mut handle) = hosted(fixture);
    {
        let mut registry = owner.lock_registry_for_test();
        assert!(registry.terminalize_one_classic_group_position_failure());
    }
    drop(handle.recv());

    let observed = handle
        .recv()
        .wait()
        .map(|batch| batch.is_some())
        .map_err(|error| error.kind());
    assert_eq!(
        observed,
        Err(GroupConsumerRecvErrorKind::Position(
            GroupConsumerPositionFailureKind::MissingOffset
        ))
    );
    stop_hosted(owner, handle);
}

fn settle_and_confirm(fixture: &mut PositionSettlementFixture) {
    for _turn in 0..2 {
        assert_eq!(
            fixture
                .registry
                .settle_one_classic_group_position(Moment::from_tick(50)),
            Ok(ClassicGroupPositionSettlementTurn::Progress)
        );
    }
}

fn assert_blocking_recv_observes_position_failure(
    fixture: PositionSettlementFixture,
    expected: GroupConsumerPositionFailureKind,
) {
    let (owner, mut handle) = hosted(fixture);
    let (result_tx, result_rx) = sync_channel(1);
    let mut timed_out = false;

    thread::scope(|scope| {
        let waiter = scope.spawn(|| {
            let observed = handle
                .recv()
                .wait()
                .map(|batch| batch.is_some())
                .map_err(|error| error.kind());
            result_tx
                .send(observed)
                .unwrap_or_else(|error| panic!("publish receive result: {error}"));
        });
        {
            let mut registry = owner.lock_registry_for_test();
            assert!(registry.terminalize_one_classic_group_position_failure());
        }
        owner.notify_recv_change();
        let observed = match result_rx.recv_timeout(Duration::from_secs(2)) {
            Ok(observed) => observed,
            Err(_timeout) => {
                timed_out = true;
                owner.close_admission();
                result_rx
                    .recv_timeout(Duration::from_secs(2))
                    .unwrap_or_else(|error| panic!("receive did not unblock after close: {error}"))
            }
        };
        waiter
            .join()
            .unwrap_or_else(|_panic| panic!("blocking receive thread panicked"));
        assert!(!timed_out, "position failure left blocking recv pending");
        assert_eq!(
            observed,
            Err(GroupConsumerRecvErrorKind::Position(expected))
        );
    });

    assert_eq!(
        immediate_failure_kind(&mut handle),
        GroupConsumerTryTakeBatchErrorKind::HostUnavailable,
        "the exact scalar must transfer only once before the retained fault"
    );
    stop_hosted(owner, handle);
}

fn assert_immediate_observation_is_one_shot(
    fixture: PositionSettlementFixture,
    expected: GroupConsumerPositionFailureKind,
) {
    let (owner, mut handle) = hosted(fixture);
    {
        let mut registry = owner.lock_registry_for_test();
        assert!(registry.terminalize_one_classic_group_position_failure());
    }
    assert_eq!(
        immediate_failure_kind(&mut handle),
        GroupConsumerTryTakeBatchErrorKind::Position(expected)
    );
    assert_eq!(
        immediate_failure_kind(&mut handle),
        GroupConsumerTryTakeBatchErrorKind::HostUnavailable
    );
    stop_hosted(owner, handle);
}

fn hosted(fixture: PositionSettlementFixture) -> (GroupConsumerShardOwner, GroupConsumerHandle) {
    let PositionSettlementFixture {
        registry,
        group_id,
        fence: _,
        deadline: _,
    } = fixture;
    let (owner, port) = GroupConsumerShardOwner::new(
        registry,
        Arc::new(crate::clock::MonotonicClock::new()),
        Arc::new(NoopWake),
    );
    let lifetime: Arc<dyn Send + Sync> = Arc::new(());
    let handle = GroupConsumerHandle {
        group_id,
        port,
        lifetime,
        _not_sync: PhantomData,
    };
    (owner, handle)
}

fn immediate_failure_kind(handle: &mut GroupConsumerHandle) -> GroupConsumerTryTakeBatchErrorKind {
    match handle.try_take_batch() {
        Err(error) => error.kind(),
        Ok(Some(_batch)) => panic!("position failure transferred a batch"),
        Ok(None) => panic!("position failure was not observable"),
    }
}

fn stop_hosted(mut owner: GroupConsumerShardOwner, handle: GroupConsumerHandle) {
    drop(handle);
    let mut registry = owner.terminal_registry();
    for entry in &mut registry.entries {
        drop(entry.fault.take());
    }
    registry
        .recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("registry recovery: {error}"));
    let commit_join = registry
        .finish_shutdown()
        .unwrap_or_else(|error| panic!("registry finish: {error}"));
    drop(registry);
    let recv_join = owner
        .stop_recv_notifier()
        .unwrap_or_else(|| panic!("group receive notifier owner"));
    commit_join
        .join_off_notifier()
        .unwrap_or_else(|error| panic!("commit notifier join: {error}"));
    recv_join
        .join_off_notifier()
        .unwrap_or_else(|error| panic!("receive notifier join: {error}"));
}

struct NoopWake;

impl super::registry_wake::GroupConsumerShardWake for NoopWake {
    fn request_group_turn(&self) -> Result<(), super::registry_wake::GroupConsumerShardWakeError> {
        Ok(())
    }
}
