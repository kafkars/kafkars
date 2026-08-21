//! Private transactional-send ownership and runtime-neutral operation contracts.

use std::{future::Future, time::Duration};

use crate::{KafkaError, Record, RecordMetadata};

use super::{TransactionEngine, TransactionSendEngine};

type SendMethod<'send, 'producer> =
    fn(
        &'send mut TransactionEngine<'producer>,
        Record,
        Duration,
    ) -> Result<TransactionSendEngine<'send, 'producer>, (Record, KafkaError)>;

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
fn private_send_reborrows_transaction_until_sole_observer_is_released() {
    fn require_send(_method: SendMethod<'_, '_>) {}
    fn require_future<T: Future<Output = Result<RecordMetadata, KafkaError>>>() {}
    fn require_wait(
        _method: fn(TransactionSendEngine<'static, 'static>) -> Result<RecordMetadata, KafkaError>,
    ) {
    }

    require_send(TransactionEngine::send);
    require_future::<TransactionSendEngine<'static, 'static>>();
    require_wait(TransactionSendEngine::wait);
    assert_not_impl!(TransactionSendEngine<'static, 'static>: Clone);
    assert_not_impl!(TransactionSendEngine<'static, 'static>: Copy);
}
