//! Linear post-core broker-rejection effect retention scenarios.

use super::classic_group_rejection_fault::{
    ClassicRejectionInstallFailure, ClassicRejectionPostCore,
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
fn empty_shape_fault_still_counts_one_shutdown_obligation() {
    let fault =
        ClassicRejectionPostCore::new([None, None], ClassicRejectionInstallFailure::EffectShape);

    assert_eq!(fault.retained_owner_count(), 1);
    assert_eq!(fault.failure(), ClassicRejectionInstallFailure::EffectShape);
}

#[test]
fn the_post_core_effect_owner_remains_linear() {
    assert_not_impl!(ClassicRejectionPostCore: Clone);
    assert_not_impl!(ClassicRejectionPostCore: Copy);
}
