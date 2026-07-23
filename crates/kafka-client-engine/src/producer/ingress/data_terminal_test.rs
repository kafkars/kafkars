//! Shard-wide terminal refusal and recovery scenarios.

use std::time::Instant;

use kafka_client_core::{Deadline, Moment};

use crate::{
    clock::OperationDeadline,
    producer::{
        admission_test::record,
        host_limits_test::{start, valid_limits},
    },
};

use super::{
    data::ProducerShardData,
    terminal::{ProducerShardPendingOwnership, ProducerShardTerminalError},
};

#[test]
fn normal_terminal_cleanup_refuses_registered_pending_ownership_without_drain() {
    let mut data = ProducerShardData::new(start(valid_limits()));
    let waiting = data
        .register_pending(record("pending"), deadline())
        .unwrap_or_else(|error| panic!("pending record should register: {error:?}"));
    data.close_admission();
    let expected = pending_ownership(&data);

    let Err(release) = data.verify_release_before_completion() else {
        panic!("release verification must refuse pending ownership")
    };
    assert_pending(release, expected);
    let Err(drain) = data.drain_terminal_mechanisms() else {
        panic!("terminal drain must refuse pending ownership")
    };
    assert_pending(drain, expected);
    let Err(final_check) = data.verify_terminal_cleanup() else {
        panic!("final verification must refuse pending ownership")
    };
    assert_pending(final_check, expected);
    let Err(stop) = data.begin_notification_shutdown() else {
        panic!("notification shutdown must refuse pending ownership")
    };
    assert_pending(stop, expected);
    assert_eq!(pending_ownership(&data), expected);
    drop(waiting);
}

#[test]
fn recovery_refuses_registered_pending_ownership_after_settling_accepted_work() {
    let mut data = ProducerShardData::new(start(valid_limits()));
    let accepted = data
        .try_admit_explicit(Moment::from_tick(1), deadline(), record("accepted"))
        .unwrap_or_else(|error| panic!("accepted record should enter core: {error:?}"));
    let waiting = data
        .register_pending(record("pending"), deadline())
        .unwrap_or_else(|error| panic!("pending record should register: {error:?}"));
    let expected = pending_ownership(&data);

    data.execution_unavailable(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("accepted work should settle before refusal: {error}"));
    assert!(accepted.into_delivery_observer().wait().is_err());
    let Err(release) = data.verify_release_before_completion() else {
        panic!("recovery verification must refuse pending ownership")
    };
    assert_pending(release, expected);
    let Err(drain) = data.drain_terminal_mechanisms() else {
        panic!("recovery drain must preserve pending ownership")
    };
    assert_pending(drain, expected);
    let Err(recovery) = data.recover_notifier() else {
        panic!("notifier recovery must refuse pending ownership")
    };
    assert_pending(recovery, expected);

    let stats = data.shard_stats();
    assert_eq!(stats.host.store.records, 0);
    assert_eq!(pending_ownership(&data), expected);
    drop(waiting);
}

fn pending_ownership(data: &ProducerShardData) -> ProducerShardPendingOwnership {
    let stats = data.shard_stats();
    ProducerShardPendingOwnership::new(
        stats.pending.records,
        stats.pending.retained_bytes,
        stats.pending.notification_permits,
    )
}

fn assert_pending(error: ProducerShardTerminalError, expected: ProducerShardPendingOwnership) {
    assert_eq!(error.pending_ownership(), Some(expected));
}

fn deadline() -> OperationDeadline {
    OperationDeadline::from_parts_for_test(Deadline::from_tick(90), Instant::now())
}
