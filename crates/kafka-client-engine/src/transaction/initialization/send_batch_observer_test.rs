//! Accepted transactional batch observer linearity and runtime-neutral contracts.

use std::future::Future;

use super::{
    TransactionBatchSendObserver, TransactionBatchSendOutcome, TransactionSendObserverError,
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
fn accepted_batch_has_one_named_linear_future_and_waiter() {
    fn require_future<
        T: Future<Output = Result<TransactionBatchSendOutcome, TransactionSendObserverError>>,
    >() {
    }
    fn require_wait(
        _method: fn(
            TransactionBatchSendObserver<'static, 'static>,
        ) -> Result<TransactionBatchSendOutcome, TransactionSendObserverError>,
    ) {
    }

    require_future::<TransactionBatchSendObserver<'static, 'static>>();
    require_wait(TransactionBatchSendObserver::wait);
    assert_not_impl!(TransactionBatchSendObserver<'static, 'static>: Clone);
    assert_not_impl!(TransactionBatchSendObserver<'static, 'static>: Copy);
}
