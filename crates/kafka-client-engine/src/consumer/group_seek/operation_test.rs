//! Compile-time terminal-observer surface contract.

use std::future::Future;

use super::{GroupConsumerSeek, GroupConsumerSeekError};

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
fn seek_is_a_send_linear_future_with_the_same_blocking_outcome() {
    fn require<T>()
    where
        T: Future<Output = Result<(), GroupConsumerSeekError>> + Send,
    {
    }
    fn wait(operation: GroupConsumerSeek<'_>) -> Result<(), GroupConsumerSeekError> {
        operation.wait()
    }

    require::<GroupConsumerSeek<'_>>();
    assert_not_impl!(GroupConsumerSeek<'_>: Clone);
    let _ = wait;
}
