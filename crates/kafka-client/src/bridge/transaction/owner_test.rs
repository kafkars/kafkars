//! Private transaction owner linearity and threading contract.

use super::TransactionalProducerEngine;

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
fn private_owner_is_sendable_unique_and_not_shared() {
    fn require_send<T: Send>() {}

    require_send::<TransactionalProducerEngine>();
    assert_not_impl!(TransactionalProducerEngine: Clone);
    assert_not_impl!(TransactionalProducerEngine: Copy);
    assert_not_impl!(TransactionalProducerEngine: Sync);
}
