//! Transaction initialization operation trait contract.

use std::future::Future;

use super::InitializeTransactionalProducer;

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
fn named_initialization_is_one_sendable_observer() {
    fn require_future<T: Future + Send>() {}

    require_future::<InitializeTransactionalProducer>();
    assert_not_impl!(InitializeTransactionalProducer: Clone);
    assert_not_impl!(InitializeTransactionalProducer: Copy);
}
