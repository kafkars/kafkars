//! Public assigned-consumer event vocabulary and linearity contracts.

use super::{
    AssignedConsumerEvent, AssignedConsumerFetchFailureKind, AssignedConsumerFetchFence,
    AssignedConsumerPositionFence,
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
fn public_fetch_event_preserves_named_scalar_facts() {
    let event = AssignedConsumerEvent::FetchFailed {
        fence: AssignedConsumerFetchFence::from_parts(
            AssignedConsumerPositionFence::from_parts("orders".to_owned(), 3, 7, 11),
            13,
        ),
        kind: AssignedConsumerFetchFailureKind::Broker(-42),
    };
    let AssignedConsumerEvent::FetchFailed { fence, kind } = event else {
        panic!("Fetch failure");
    };

    assert_eq!(fence.position().topic(), "orders");
    assert_eq!(fence.position().partition(), 3);
    assert_eq!(fence.position().assignment_epoch(), 7);
    assert_eq!(fence.position().position_epoch(), 11);
    assert_eq!(fence.fetch_revision(), 13);
    assert_eq!(kind, AssignedConsumerFetchFailureKind::Broker(-42));
    assert_not_impl!(AssignedConsumerEvent: Clone);
    assert_not_impl!(AssignedConsumerEvent: Copy);
}
