//! Stable scalar event vocabulary and accessor scenarios.

use std::sync::Arc;

use super::{
    AssignedConsumerEvent, AssignedConsumerFetchFailure, AssignedConsumerFetchFailureKind,
    AssignedConsumerFetchFence, AssignedConsumerPositionFence,
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
fn fetch_event_exposes_only_stable_named_scalar_facts() {
    let event = AssignedConsumerEvent::FetchFailed(AssignedConsumerFetchFailure {
        fence: AssignedConsumerFetchFence {
            position: AssignedConsumerPositionFence {
                topic: Arc::from("orders"),
                partition: 3,
                assignment_epoch: 7,
                position_epoch: 11,
            },
            fetch_revision: 13,
        },
        kind: AssignedConsumerFetchFailureKind::Broker(-42),
    });
    let AssignedConsumerEvent::FetchFailed(failure) = event else {
        panic!("Fetch event");
    };

    assert_eq!(failure.fence().position().topic(), "orders");
    assert_eq!(failure.fence().position().partition(), 3);
    assert_eq!(failure.fence().position().assignment_epoch(), 7);
    assert_eq!(failure.fence().position().position_epoch(), 11);
    assert_eq!(failure.fence().fetch_revision(), 13);
    assert_eq!(
        failure.kind(),
        AssignedConsumerFetchFailureKind::Broker(-42)
    );
    assert_not_impl!(AssignedConsumerEvent: Clone);
    assert_not_impl!(AssignedConsumerEvent: Copy);
}
