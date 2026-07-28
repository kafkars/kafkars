//! Accepted-close observer shape and linearity evidence.

use std::future::Future;

use super::GroupConsumerClose;

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
fn close_observer_is_send_future_with_a_blocking_adapter() {
    fn require<T: Send + Future<Output = Result<(), super::GroupConsumerCloseError>>>() {}
    fn require_blocking_adapter(
        _adapter: fn(GroupConsumerClose) -> Result<(), super::GroupConsumerCloseError>,
    ) {
    }
    require::<GroupConsumerClose>();
    assert_not_impl!(GroupConsumerClose: Clone);
    require_blocking_adapter(GroupConsumerClose::wait);
}
