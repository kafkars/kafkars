//! Linearity, handle binding, and no-admission-drop tests for assignment capture.

use std::{sync::Arc, time::Duration};

use super::{
    AssignedConsumerAssignment, AssignedConsumerAssignmentCapture, AssignedConsumerHandle,
    AssignedConsumerStartPosition, AssignedConsumerTryReplaceAssignmentError,
    claim::AssignedConsumerClaimSlot, shard_test::setup,
};

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
fn capture_is_linear_and_owns_the_only_handle_bound_admission_method() {
    fn require_capture(
        _capture: for<'handle> fn(
            &'handle mut AssignedConsumerHandle,
            Duration,
        ) -> Result<
            AssignedConsumerAssignmentCapture<'handle>,
            AssignedConsumerTryReplaceAssignmentError,
        >,
    ) {
    }
    require_capture(AssignedConsumerHandle::capture_replace_assignment);
    assert_not_impl!(AssignedConsumerAssignmentCapture<'static>: Clone);
    assert_not_impl!(AssignedConsumerAssignmentCapture<'static>: Copy);
}

#[test]
fn dropping_capture_changes_no_assignment_or_wake_state() {
    let (owner, port, wake) = setup();
    let (slot, _closer) = AssignedConsumerClaimSlot::create_for_engine(port);
    let lifetime: Arc<dyn Send + Sync> = Arc::new(());
    let mut handle = slot
        .claim(lifetime)
        .unwrap_or_else(|error| panic!("claim assigned consumer: {error}"));

    let capture = handle
        .capture_replace_assignment(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("capture assignment: {error}"));
    drop(capture);

    let partition_count = owner
        .try_with_owner(|assigned| assigned.topics.partitions().len())
        .unwrap_or_else(|error| panic!("assigned owner: {error:?}"));
    assert_eq!(partition_count, 0);
    assert_eq!(wake.count(), 0);
}

#[test]
fn consumed_capture_admits_exactly_one_assignment() {
    let (_owner, port, wake) = setup();
    let (slot, _closer) = AssignedConsumerClaimSlot::create_for_engine(port);
    let lifetime: Arc<dyn Send + Sync> = Arc::new(());
    let mut handle = slot
        .claim(lifetime)
        .unwrap_or_else(|error| panic!("claim assigned consumer: {error}"));
    let capture = handle
        .capture_replace_assignment(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("capture assignment: {error}"));
    let entry =
        AssignedConsumerAssignment::try_new("orders", 0, AssignedConsumerStartPosition::Beginning)
            .unwrap_or_else(|error| panic!("assignment input: {error}"));

    let accepted = capture
        .try_replace_assignment(vec![entry])
        .unwrap_or_else(|error| panic!("admit assignment: {error}"));

    assert_eq!(accepted.epoch().get(), 1);
    assert_eq!(wake.count(), 1);
}
