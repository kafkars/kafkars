//! Aggregate shard byte, permit, and close-state scenarios.

use std::time::Instant;

use kafka_client_core::{AdmissionRejection, Deadline, Moment};

use super::{
    data::ProducerShardData,
    terminal::{ProducerShardPendingOwnership, ProducerShardTerminalError},
};
use crate::{
    clock::OperationDeadline,
    producer::{
        ProducerRejectionReason,
        admission::ProducerAdmissionFailure,
        admission_test::record,
        host_limits_test::{start, valid_limits},
        pending::PendingAdmissionRejectionReason,
    },
};

#[test]
fn construction_installs_one_live_pending_population_from_host_capacity() {
    let limits = valid_limits();
    let data = ProducerShardData::new(start(limits));
    let stats = data.shard_stats();

    assert_eq!(stats.pending.records, 0);
    assert_eq!(stats.pending.retained_bytes, 0);
    assert_eq!(stats.pending.notification_permits, 0);
    assert!(stats.pending.accepting);
    assert!(stats.accepting);
    assert_eq!(
        stats.pending_notification_capacity,
        limits.pending_record_capacity
    );
    assert_eq!(stats.retained_byte_limit, limits.retained_bytes);
}

#[test]
fn accepted_and_pending_records_share_one_retained_byte_ceiling() {
    let mut limits = valid_limits();
    limits.retained_bytes = 7;
    let mut data = ProducerShardData::new(start(limits));
    let accepted = data
        .try_admit_explicit(Moment::from_tick(1), deadline(), record("one"))
        .unwrap_or_else(|error| panic!("first record should be accepted: {error:?}"));
    let rejected = match data.register_pending(record("two"), deadline()) {
        Err(rejected) => rejected,
        Ok(_pending) => panic!("aggregate application-byte ceiling should reject pending record"),
    };

    assert_eq!(
        rejected.reason(),
        PendingAdmissionRejectionReason::ByteCapacity
    );
    assert_eq!(data.shard_stats().aggregate_retained_bytes, 4);
    drop(accepted);
}

#[test]
fn pending_bytes_fence_immediate_admission_under_the_same_owner() {
    let mut limits = valid_limits();
    limits.retained_bytes = 7;
    let mut data = ProducerShardData::new(start(limits));
    let pending = data
        .register_pending(record("one"), deadline())
        .unwrap_or_else(|error| panic!("pending record should fit: {error:?}"));

    let admitted = data.try_admit_explicit(Moment::from_tick(1), deadline(), record("two"));
    let Err(ProducerAdmissionFailure::Rejected(rejected)) = admitted else {
        panic!("pending bytes should fence aggregate immediate admission")
    };
    assert_eq!(
        rejected.reason(),
        ProducerRejectionReason::Store(crate::producer::ProducerStoreError::ByteCapacity)
    );
    let stats = data.shard_stats();
    assert_eq!(stats.aggregate_retained_bytes, 4);
    assert_eq!(stats.pending.notification_permits, 1);
    assert_eq!(stats.host.pending_notification_permits, 1);
    drop(pending);
}

#[test]
fn close_atomically_stops_pending_and_core_admission() {
    let mut limits = valid_limits();
    limits.retained_bytes = 7;
    let mut data = ProducerShardData::new(start(limits));
    let waiting = data
        .register_pending(record("one"), deadline())
        .unwrap_or_else(|error| panic!("pre-close pending record should fit: {error:?}"));
    data.close_admission();

    let pending = match data.register_pending(record("pending"), deadline()) {
        Err(rejected) => rejected,
        Ok(_pending) => panic!("pending admission should close with core admission"),
    };
    assert_eq!(pending.reason(), PendingAdmissionRejectionReason::Closed);

    let accepted = data.try_admit_explicit(Moment::from_tick(1), deadline(), record("core"));
    let Err(ProducerAdmissionFailure::Rejected(rejected)) = accepted else {
        panic!("core admission should reject after aggregate close")
    };
    assert_eq!(
        rejected.reason(),
        ProducerRejectionReason::Core(AdmissionRejection::Closed)
    );
    assert!(!data.shard_stats().pending.accepting);
    assert!(!data.shard_stats().accepting);
    drop(waiting);
}

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
    let Err(stop) = data.stop_notifier() else {
        panic!("notifier stop must refuse pending ownership")
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
