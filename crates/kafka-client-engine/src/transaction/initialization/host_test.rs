//! Atomic admission, absolute-deadline, recovery, and byte-accounting scenarios.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

use kafka_client_core::{Deadline, Moment, TransactionInitializationPlan};

use crate::clock::OperationDeadline;

use super::{
    TransactionInitializationDeliveryStatus, TransactionInitializationFailureKind,
    TransactionInitializationHost, TransactionInitializationOutcome,
    TransactionInitializationRequest, host::TRANSACTION_INITIALIZATION_OPERATION_BYTES,
};

#[test]
fn admission_reserves_terminal_and_transactional_id_envelope_before_acceptance() {
    let mut host = TransactionInitializationHost::start()
        .unwrap_or_else(|error| panic!("start transaction host: {error}"));
    let admission = host
        .try_admit(
            Moment::from_tick(1),
            deadline(10),
            request(),
            plan(),
            std::sync::Arc::new(()),
        )
        .unwrap_or_else(|(error, _request)| panic!("admit transaction: {error:?}"));
    assert!(admission.fault.is_none());
    assert_eq!(host.unsettled(), 1);
    assert_eq!(
        host.retained_bytes_for_test(),
        TRANSACTION_INITIALIZATION_OPERATION_BYTES
    );

    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover definitely-unsent request: {error}"));
    let TransactionInitializationOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovered terminal: {error}"))
    else {
        panic!("recovery must fail initialization");
    };
    assert_eq!(
        (failure.kind, failure.delivery),
        (
            TransactionInitializationFailureKind::DriverRejected,
            TransactionInitializationDeliveryStatus::NotSent,
        )
    );
    let _reclaimed = host
        .reclaim_for_test()
        .unwrap_or_else(|error| panic!("reclaim terminal: {error}"));
    assert_eq!(host.retained_bytes_for_test(), 0);
    stop(host);
}

#[test]
fn elapsed_original_operation_deadline_never_needs_driver_submission() {
    let mut host = TransactionInitializationHost::start()
        .unwrap_or_else(|error| panic!("start transaction host: {error}"));
    let admission = host
        .try_admit(
            Moment::from_tick(10),
            deadline(10),
            request(),
            plan(),
            std::sync::Arc::new(()),
        )
        .unwrap_or_else(|(error, _request)| panic!("admit elapsed transaction: {error:?}"));
    let TransactionInitializationOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe elapsed terminal: {error}"))
    else {
        panic!("deadline terminal expected");
    };
    assert_eq!(
        (failure.kind, failure.delivery),
        (
            TransactionInitializationFailureKind::DeadlineElapsed,
            TransactionInitializationDeliveryStatus::NotSent,
        )
    );
    stop(host);
}

#[test]
fn shutdown_fences_live_owner_and_reclaims_its_byte_envelope_before_handle_drop() {
    let mut host = TransactionInitializationHost::start()
        .unwrap_or_else(|error| panic!("start transaction host: {error}"));
    let admission = host
        .try_admit(
            Moment::from_tick(1),
            deadline(10),
            request(),
            plan(),
            std::sync::Arc::new(()),
        )
        .unwrap_or_else(|(error, _request)| panic!("admit transaction: {error:?}"));
    host.initialize_for_test(41, 3)
        .unwrap_or_else(|error| panic!("settle transaction identity: {error}"));
    let TransactionInitializationOutcome::Initialized(handle) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe initialized owner: {error}"))
    else {
        panic!("broker identity must create a unique transactional owner");
    };
    assert!(handle.is_active());
    assert_eq!(
        host.retained_bytes_for_test(),
        TRANSACTION_INITIALIZATION_OPERATION_BYTES
    );

    let join = host
        .finish_shutdown()
        .unwrap_or_else(|error| panic!("stop transaction host: {error}"));
    assert!(!handle.is_active());
    assert_eq!(host.retained_bytes_for_test(), 0);
    drop(handle);
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("join transaction notifier: {error}"));
}

#[test]
fn abandoned_success_releases_its_live_owner_without_engine_shutdown() {
    let mut host = TransactionInitializationHost::start()
        .unwrap_or_else(|error| panic!("start transaction host: {error}"));
    let admission = host
        .try_admit(
            Moment::from_tick(1),
            deadline(10),
            request(),
            plan(),
            Arc::new(()),
        )
        .unwrap_or_else(|(error, _request)| panic!("admit transaction: {error:?}"));
    host.initialize_for_test(41, 3)
        .unwrap_or_else(|error| panic!("settle transaction identity: {error}"));
    drop(admission.observer);
    let wait_deadline = Instant::now() + std::time::Duration::from_secs(1);
    while !host
        .reclaim_for_test()
        .unwrap_or_else(|error| panic!("reclaim abandoned success: {error}"))
    {
        assert!(
            Instant::now() < wait_deadline,
            "abandoned success should become reclaimable"
        );
        std::thread::yield_now();
    }
    assert!(
        host.release_owner_for_test()
            .unwrap_or_else(|error| panic!("release abandoned owner: {error}"))
    );
    assert_eq!(host.retained_bytes_for_test(), 0);
    stop(host);
}

#[test]
fn unobserved_success_keeps_engine_lifetime_only_on_the_external_observer() {
    let mut host = TransactionInitializationHost::start()
        .unwrap_or_else(|error| panic!("start transaction host: {error}"));
    let lifetime_dropped = Arc::new(AtomicBool::new(false));
    let lifetime: Arc<dyn Send + Sync> = Arc::new(LifetimeWitness {
        dropped: Arc::clone(&lifetime_dropped),
    });
    let admission = host
        .try_admit(
            Moment::from_tick(1),
            deadline(10),
            request(),
            plan(),
            lifetime,
        )
        .unwrap_or_else(|(error, _request)| panic!("admit transaction: {error:?}"));
    host.initialize_for_test(41, 3)
        .unwrap_or_else(|error| panic!("settle transaction identity: {error}"));
    let join = host
        .finish_shutdown()
        .unwrap_or_else(|error| panic!("stop transaction host: {error}"));
    assert!(!lifetime_dropped.load(Ordering::Acquire));

    drop(admission.observer);
    assert!(lifetime_dropped.load(Ordering::Acquire));
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("join transaction notifier: {error}"));
}

fn request() -> TransactionInitializationRequest {
    TransactionInitializationRequest::new("invoice-writer".to_owned(), 45_000)
}

fn plan() -> TransactionInitializationPlan {
    TransactionInitializationPlan::new(45_000)
        .unwrap_or_else(|error| panic!("valid transaction plan: {error}"))
}

fn deadline(tick: u64) -> OperationDeadline {
    OperationDeadline::from_parts_for_test(Deadline::from_tick(tick), Instant::now())
}

fn stop(mut host: TransactionInitializationHost) {
    host.close_admission();
    if host.unsettled() == 0 {
        let join = host
            .finish_shutdown()
            .unwrap_or_else(|error| panic!("stop transaction notifier: {error}"));
        join.join_off_notifier()
            .unwrap_or_else(|error| panic!("join transaction notifier: {error}"));
    } else if let Some(join) = host.take_notifier() {
        join.join_off_notifier()
            .unwrap_or_else(|error| panic!("join transaction notifier: {error}"));
    }
}

struct LifetimeWitness {
    dropped: Arc<AtomicBool>,
}

impl Drop for LifetimeWitness {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::Release);
    }
}
