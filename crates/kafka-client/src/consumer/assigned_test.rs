//! Public assigned-consumer linearity and threading contract.

use super::{AssignedConsumer, CloseAssignedConsumer};

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
fn assigned_consumer_is_send_linear_and_not_shared() {
    fn require_send<T: Send>() {}

    require_send::<AssignedConsumer>();
    assert_not_impl!(AssignedConsumer: Clone);
    assert_not_impl!(AssignedConsumer: Copy);
    assert_not_impl!(AssignedConsumer: Sync);
}

#[test]
fn close_rejection_retains_the_unique_consumer_for_retry() {
    fn require_close(
        _close: fn(&mut AssignedConsumer) -> Result<CloseAssignedConsumer, crate::KafkaError>,
    ) {
    }

    require_close(AssignedConsumer::try_close);
}
