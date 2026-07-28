//! Unique transactional-owner type-shape evidence.

use super::TransactionalOwnerHandle;

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
fn initialized_owner_is_sendable_linear_and_not_shared() {
    fn require_send<T: Send>() {}

    require_send::<TransactionalOwnerHandle>();
    assert_not_impl!(TransactionalOwnerHandle: Clone);
    assert_not_impl!(TransactionalOwnerHandle: Copy);
    assert_not_impl!(TransactionalOwnerHandle: Sync);
}
