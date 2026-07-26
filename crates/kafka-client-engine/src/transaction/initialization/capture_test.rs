//! Linear transaction-initialization capture trait contract.

use super::TransactionInitializationCapture;

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
fn captured_deadline_is_linear_and_sendable() {
    fn require_send<T: Send>() {}

    require_send::<TransactionInitializationCapture>();
    assert_not_impl!(TransactionInitializationCapture: Clone);
    assert_not_impl!(TransactionInitializationCapture: Copy);
}
