//! Public transactional-offset observer contract.

use std::future::Future;

use crate::KafkaError;

use super::SendTransactionOffsets;

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
fn accepted_offsets_are_runtime_neutral_and_linear() {
    fn require_future<T: Future<Output = Result<(), KafkaError>> + Send>() {}
    fn require_wait(
        _method: fn(SendTransactionOffsets<'static, 'static>) -> Result<(), KafkaError>,
    ) {
    }
    fn require_wake(_method: fn(&SendTransactionOffsets<'static, 'static>) -> bool) {}

    require_future::<SendTransactionOffsets<'static, 'static>>();
    require_wait(SendTransactionOffsets::wait);
    require_wake(SendTransactionOffsets::wake_failed);
    assert_not_impl!(SendTransactionOffsets<'static, 'static>: Clone);
    assert_not_impl!(SendTransactionOffsets<'static, 'static>: Copy);
}
