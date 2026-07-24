//! Trait and public-shape contracts for the unique assigned-consumer handle.

use super::handle::AssignedConsumerHandle;

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
