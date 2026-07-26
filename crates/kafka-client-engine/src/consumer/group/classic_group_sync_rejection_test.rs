//! Linear Sync rejection result ownership scenarios.

use super::classic_group_sync_rejection::ClassicSyncRejectionFailure;

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
fn sync_rejection_failure_remains_a_single_linear_owner() {
    assert_not_impl!(ClassicSyncRejectionFailure: Clone);
    assert_not_impl!(ClassicSyncRejectionFailure: Copy);
}
