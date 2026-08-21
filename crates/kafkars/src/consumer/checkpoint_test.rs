//! Public group-checkpoint linearity and observation contract.

use super::Checkpoint;

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
fn checkpoint_is_send_linear_and_exposes_stable_identity() {
    fn require_send<T: Send>() {}
    fn contract(checkpoint: &Checkpoint) {
        let _: &str = checkpoint.topic();
        let _: i32 = checkpoint.partition();
        let _: i64 = checkpoint.next_offset();
    }
    require_send::<Checkpoint>();
    assert_not_impl!(Checkpoint: Clone);
    assert_not_impl!(Checkpoint: Copy);
    let _ = contract as fn(&Checkpoint);
}
