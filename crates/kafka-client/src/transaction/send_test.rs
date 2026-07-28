//! Accepted transactional-send future, wait, and wake-status contracts.

use std::future::Future;

use crate::{KafkaError, RecordMetadata};

use super::SendTransactionRecord;

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
fn accepted_send_is_one_named_linear_future_and_waiter() {
    fn require_future<T: Future<Output = Result<RecordMetadata, KafkaError>>>() {}
    fn require_wait(
        _method: fn(SendTransactionRecord<'static, 'static>) -> Result<RecordMetadata, KafkaError>,
    ) {
    }
    fn require_wake(_method: fn(&SendTransactionRecord<'static, 'static>) -> bool) {}

    require_future::<SendTransactionRecord<'static, 'static>>();
    require_wait(SendTransactionRecord::wait);
    require_wake(SendTransactionRecord::wake_failed);
    assert_not_impl!(SendTransactionRecord<'static, 'static>: Clone);
    assert_not_impl!(SendTransactionRecord<'static, 'static>: Copy);
}
