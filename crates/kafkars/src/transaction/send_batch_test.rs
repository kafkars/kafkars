//! Accepted transactional batch future, wait, and wake-status contracts.

use std::future::Future;

use crate::KafkaError;

use super::{SendTransactionBatch, TransactionBatchMetadata};

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
fn accepted_batch_is_one_named_linear_future_and_waiter() {
    fn require_future<T: Future<Output = Result<TransactionBatchMetadata, KafkaError>>>() {}
    fn require_wait(
        _method: fn(
            SendTransactionBatch<'static, 'static>,
        ) -> Result<TransactionBatchMetadata, KafkaError>,
    ) {
    }
    fn require_wake(_method: fn(&SendTransactionBatch<'static, 'static>) -> bool) {}

    require_future::<SendTransactionBatch<'static, 'static>>();
    require_wait(SendTransactionBatch::wait);
    require_wake(SendTransactionBatch::wake_failed);
    assert_not_impl!(SendTransactionBatch<'static, 'static>: Clone);
    assert_not_impl!(SendTransactionBatch<'static, 'static>: Copy);
}
