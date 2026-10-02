//! Public share-close category and certainty-preserving ownership scenarios.

use kafka_client_core::ShareGroupHeartbeatFailure;
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
};

use super::{
    port::ShareClosePortError,
    public_close::{
        ShareConsumerCloseAdmissionErrorKind, ShareConsumerCloseErrorKind, close_admission_kind,
        terminal_error,
    },
    registry_close::ShareConsumerCloseAdmissionError as RegistryCloseError,
    shard::ShareConsumerShardLockError,
};

#[test]
fn only_pre_admission_contention_is_publicly_retryable_by_ownership() {
    assert_eq!(
        close_admission_kind(ShareClosePortError::Lock(
            ShareConsumerShardLockError::Contended,
        )),
        ShareConsumerCloseAdmissionErrorKind::Contended
    );
    assert_eq!(
        close_admission_kind(ShareClosePortError::Registry(
            RegistryCloseError::AlreadyClosing,
        )),
        ShareConsumerCloseAdmissionErrorKind::Unavailable
    );
}

#[test]
fn terminal_mapping_preserves_exact_broker_code_and_deadline() {
    let broker = terminal_error(ShareGroupHeartbeatFailure::Broker(27));
    assert_eq!(broker.kind(), ShareConsumerCloseErrorKind::BrokerRejected);
    assert_eq!(broker.broker_code(), Some(27));
    let deadline = terminal_error(ShareGroupHeartbeatFailure::DeadlineElapsed);
    assert_eq!(
        deadline.kind(),
        ShareConsumerCloseErrorKind::DeadlineElapsed
    );
    assert_eq!(deadline.broker_code(), None);
}

#[test]
fn shutdown_recovery_preserves_the_selected_share_close_failure() {
    for failure in [
        ShareGroupHeartbeatFailure::DeadlineElapsed,
        ShareGroupHeartbeatFailure::Broker(-731),
    ] {
        let (mut registry, group_id, clock, _start) =
            super::registry_fetch_routing_test::registry_with_routable_membership();
        let capture = clock
            .capture_deadline_after(std::time::Duration::from_secs(1))
            .unwrap_or_else(|error| panic!("deadline: {error:?}"));
        let observer = registry
            .begin_explicit_close(group_id, capture)
            .unwrap_or_else(|error| panic!("close: {error:?}"));
        let terminal = super::close_state::ShareConsumerCloseTerminal::Failed(failure);
        registry
            .entry_mut(group_id)
            .and_then(super::entry::ShareConsumerEntry::close_mut)
            .unwrap_or_else(|| panic!("close owner"))
            .retain_share_close_terminal(terminal)
            .unwrap_or_else(|error| panic!("terminal: {error:?}"));
        registry
            .recover_after_driver_shutdown()
            .unwrap_or_else(|error| panic!("recover: {error:?}"));
        assert_eq!(observer.wait(), Ok(terminal));
        assert_eq!(registry.retained_name_bytes(), 0);
    }
}

#[test]
fn retained_first_share_close_does_not_hide_a_second_close_failure() {
    let (mut registry, first, clock, _start) =
        super::registry_fetch_routing_test::registry_with_routable_membership();
    assert!(
        registry
            .entry_mut(first)
            .unwrap_or_else(|| panic!("first"))
            .fetch_mut()
            .install_sessions(super::fetch_session_set::owner_test::staged_session_set_for_test(41))
            .is_none()
    );
    let delivery = registry
        .take_delivery(first, kafka_client_core::Moment::from_tick(8))
        .unwrap_or_else(|error| panic!("delivery: {error:?}"))
        .unwrap_or_else(|| panic!("staged delivery"));
    let second = registry
        .try_register(
            Arc::from("workers-2"),
            None,
            vec![Arc::from("events")],
            crate::EngineShareConsumerFetchConfig::default(),
        )
        .unwrap_or_else(|_error| panic!("second"));
    let capture = clock
        .capture_deadline_after(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("deadline: {error:?}"));
    let first_observer = registry
        .begin_explicit_close(first, capture)
        .unwrap_or_else(|error| panic!("first close: {error:?}"));
    let mut second_observer = registry
        .begin_explicit_close(second, capture)
        .unwrap_or_else(|error| panic!("second close: {error:?}"));
    let elapsed = kafka_client_core::Moment::from_tick(capture.deadline().tick());
    for _turn in 0..4 {
        registry
            .turn_one_close(elapsed)
            .unwrap_or_else(|error| panic!("close turn: {error:?}"));
    }
    let mut context = Context::from_waker(Waker::noop());
    let bound = Instant::now() + Duration::from_secs(1);
    let observed = loop {
        let observed = Pin::new(&mut second_observer).poll(&mut context);
        if observed.is_ready() || Instant::now() >= bound {
            break observed;
        }
        std::thread::yield_now();
    };
    assert!(
        registry
            .entry(first)
            .is_some_and(|entry| entry.fetch().sessions().is_some())
    );
    registry
        .reclaim_delivery(delivery)
        .unwrap_or_else(|_failure| panic!("exact reclaim"));
    registry
        .recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover: {error:?}"));
    drop(first_observer);
    assert_eq!(registry.retained_name_bytes(), 0);
    assert_eq!(
        observed,
        Poll::Ready(Ok(super::close_state::ShareConsumerCloseTerminal::Failed(
            ShareGroupHeartbeatFailure::DeadlineElapsed
        )))
    );
}
