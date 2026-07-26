//! Unique transactional producer threading and lifecycle contract.

use super::TransactionalProducer;

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
fn initialized_owner_is_sendable_unique_and_not_shared() {
    fn require_send<T: Send>() {}

    require_send::<TransactionalProducer>();
    assert_not_impl!(TransactionalProducer: Clone);
    assert_not_impl!(TransactionalProducer: Copy);
    assert_not_impl!(TransactionalProducer: Sync);
}

#[test]
fn initialized_owner_exposes_only_idle_identity_and_close() {
    fn require_id(_method: fn(&TransactionalProducer) -> &str) {}
    fn require_active(_method: fn(&TransactionalProducer) -> bool) {}
    fn require_close(_method: fn(TransactionalProducer)) {}

    require_id(TransactionalProducer::transactional_id);
    require_active(TransactionalProducer::is_active);
    require_close(TransactionalProducer::close);
}
