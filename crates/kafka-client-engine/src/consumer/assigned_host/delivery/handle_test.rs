//! Public handle shape and immediate observation scenarios.

use std::{sync::Arc, time::Duration};

use crate::consumer::assigned_host::{
    self, AssignedConsumerAssignment, AssignedConsumerStartPosition,
    delivery::{
        AssignedConsumerBatch, AssignedConsumerTryTakeBatchError,
        AssignedConsumerTryTakeBatchErrorKind,
    },
    shard_test::setup,
};
use assigned_host::{claim::AssignedConsumerClaimSlot, handle::AssignedConsumerHandle};

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
fn batch_and_handle_observation_are_public_linear_shapes() {
    fn require_take(
        _take: fn(
            &mut AssignedConsumerHandle,
        )
            -> Result<Option<AssignedConsumerBatch>, AssignedConsumerTryTakeBatchError>,
    ) {
    }
    fn require_send<T: Send>() {}

    require_take(AssignedConsumerHandle::try_take_batch);
    require_send::<AssignedConsumerBatch>();
    assert_not_impl!(AssignedConsumerBatch: Clone);
    assert_not_impl!(AssignedConsumerBatch: Copy);
}

#[test]
fn immediate_observation_reports_none_without_starting_fetch_work() {
    let (_owner, port, _wake) = setup();
    let (slot, _closer) = AssignedConsumerClaimSlot::create_for_engine(port);
    let lifetime: Arc<dyn Send + Sync> = Arc::new(());
    let mut handle = slot
        .claim(lifetime)
        .unwrap_or_else(|error| panic!("claim assigned consumer: {error}"));

    assert!(
        handle
            .try_take_batch()
            .unwrap_or_else(|error| panic!("observe ready delivery: {error}"))
            .is_none()
    );
}

#[test]
fn contention_and_closed_admission_reject_before_lease_transfer() {
    let (owner, port, _wake) = setup();
    let (slot, closer) = AssignedConsumerClaimSlot::create_for_engine(port);
    let lifetime: Arc<dyn Send + Sync> = Arc::new(());
    let mut handle = slot
        .claim(lifetime)
        .unwrap_or_else(|error| panic!("claim assigned consumer: {error}"));
    let guard = owner.lock_for_test();
    let contended = handle
        .try_take_batch()
        .err()
        .unwrap_or_else(|| panic!("held owner must reject observation"));
    assert_eq!(
        contended.kind(),
        AssignedConsumerTryTakeBatchErrorKind::Contended
    );
    drop(guard);

    closer
        .close()
        .unwrap_or_else(|error| panic!("close assigned admission: {error:?}"));
    let closed = handle
        .try_take_batch()
        .err()
        .unwrap_or_else(|| panic!("closed admission must reject observation"));
    assert_eq!(closed.kind(), AssignedConsumerTryTakeBatchErrorKind::Closed);
}

#[test]
fn pending_assignment_effect_fences_observation() {
    let (_owner, port, _wake) = setup();
    let (slot, _closer) = AssignedConsumerClaimSlot::create_for_engine(port);
    let lifetime: Arc<dyn Send + Sync> = Arc::new(());
    let mut handle = slot
        .claim(lifetime)
        .unwrap_or_else(|error| panic!("claim assigned consumer: {error}"));
    let entry =
        AssignedConsumerAssignment::try_new("orders", 0, AssignedConsumerStartPosition::Beginning)
            .unwrap_or_else(|error| panic!("assignment entry: {error}"));
    let _accepted = handle
        .try_replace_assignment(vec![entry], Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("replace assignment: {error}"));

    let pending = handle
        .try_take_batch()
        .err()
        .unwrap_or_else(|| panic!("pending effects must fence observation"));
    assert_eq!(
        pending.kind(),
        AssignedConsumerTryTakeBatchErrorKind::Pending
    );
}
