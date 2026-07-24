//! Trait, acceptance, and public-shape contracts for one assigned consumer.

use std::sync::Arc;

use super::{
    claim::AssignedConsumerClaimSlot,
    handle::AssignedConsumerHandle,
    result::{
        AssignedConsumerAcceptedFaultKind, AssignedConsumerTryCloseAccepted,
        AssignedConsumerTryCloseError,
    },
    shard::AssignedConsumerShardOwner,
    shard_test::{FailingWake, setup},
};
use crate::clock::MonotonicClock;

macro_rules! assert_not_impl {
    ($type:ty: $trait:path) => {
        const _: fn() = || {
            struct Implemented;
            trait AmbiguousIfImplemented<A> {
                fn check() {}
            }
            impl<T: ?Sized> AmbiguousIfImplemented<()> for T {}
            impl<T: ?Sized + $trait> AmbiguousIfImplemented<Implemented> for T {}
            let _ = <$type as AmbiguousIfImplemented<_>>::check;
        };
    };
}

#[test]
fn assigned_consumer_handle_is_send_but_linear_and_not_shared() {
    fn require_send<T: Send>() {}

    require_send::<AssignedConsumerHandle>();
    assert_not_impl!(AssignedConsumerHandle: Clone);
    assert_not_impl!(AssignedConsumerHandle: Copy);
    assert_not_impl!(AssignedConsumerHandle: Sync);
}

#[test]
fn close_observation_and_acceptance_are_send_single_owner_values() {
    fn require_future<T>()
    where
        T: std::future::Future<
                Output = Result<(), super::close_observer::AssignedConsumerCloseObserverError>,
            > + Send,
    {
    }

    require_future::<super::close_observer::AssignedConsumerCloseObserver>();
    assert_not_impl!(super::close_observer::AssignedConsumerCloseObserver: Clone);
    assert_not_impl!(super::close_observer::AssignedConsumerCloseObserver: Copy);
    assert_not_impl!(AssignedConsumerTryCloseAccepted: Clone);
    assert_not_impl!(AssignedConsumerTryCloseAccepted: Copy);
}

#[test]
fn close_is_a_public_mutating_admission_boundary() {
    fn require_close(
        _close: fn(
            &mut AssignedConsumerHandle,
        )
            -> Result<AssignedConsumerTryCloseAccepted, AssignedConsumerTryCloseError>,
    ) {
    }

    require_close(AssignedConsumerHandle::try_close);
}

#[test]
fn public_close_accepts_once_and_observes_core_authorized_success() {
    let (owner, port, _wake) = setup();
    let (slot, _closer) = AssignedConsumerClaimSlot::create_for_engine(port);
    let lifetime: Arc<dyn Send + Sync> = Arc::new(());
    let mut handle = slot
        .claim(lifetime)
        .unwrap_or_else(|error| panic!("claim assigned consumer: {error}"));

    let accepted = handle
        .try_close()
        .unwrap_or_else(|error| panic!("accept close: {error}"));
    assert_eq!(accepted.fault(), None);
    let observer = accepted.into_observer();
    let mut driver = super::super::assigned_owner_test::driver();
    for _attempt in 0..8 {
        let complete = owner
            .try_with_owner(|assigned| {
                let _turn = assigned.turn(&driver);
                assigned.close_completed() && assigned.unsettled() == 0
            })
            .unwrap_or_else(|error| panic!("assigned owner turn: {error:?}"));
        if complete {
            break;
        }
    }
    super::super::assigned_owner_test::shutdown(&mut driver);

    assert_eq!(observer.wait(), Ok(()));
}

#[test]
fn accepted_close_retains_wake_failure_without_becoming_rejected() {
    let clock = Arc::new(MonotonicClock::new());
    let wake = Arc::new(FailingWake);
    let (_owner, port) = AssignedConsumerShardOwner::new_for_test(
        clock,
        super::super::assigned_owner_test::settings(),
        super::super::assigned_owner_test::limits(2),
        wake,
    )
    .unwrap_or_else(|error| panic!("assigned shard: {error:?}"));
    let (slot, _closer) = AssignedConsumerClaimSlot::create_for_engine(port);
    let lifetime: Arc<dyn Send + Sync> = Arc::new(());
    let mut handle = slot
        .claim(lifetime)
        .unwrap_or_else(|error| panic!("claim assigned consumer: {error}"));

    let accepted = handle
        .try_close()
        .unwrap_or_else(|error| panic!("close remains accepted: {error}"));

    assert_eq!(
        accepted.fault(),
        Some(AssignedConsumerAcceptedFaultKind::Wake)
    );
    drop(accepted.into_observer());
}

#[test]
fn shutdown_fence_rejects_close_before_ownership_crosses() {
    let (_owner, port, _wake) = setup();
    let (slot, closer) = AssignedConsumerClaimSlot::create_for_engine(port);
    let lifetime: Arc<dyn Send + Sync> = Arc::new(());
    let mut handle = slot
        .claim(lifetime)
        .unwrap_or_else(|error| panic!("claim assigned consumer: {error}"));
    closer
        .close()
        .unwrap_or_else(|error| panic!("close admission: {error:?}"));

    let error = handle
        .try_close()
        .err()
        .unwrap_or_else(|| panic!("shutdown fence should reject close"));

    assert_eq!(
        error.kind(),
        super::result::AssignedConsumerTryCloseErrorKind::Closed
    );
}
