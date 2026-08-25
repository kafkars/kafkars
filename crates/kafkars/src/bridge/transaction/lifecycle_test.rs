//! Private transaction lifecycle threading, linearity, and signature contracts.

use std::{future::Future, time::Instant};

use crate::KafkaError;

use super::{TransactionEndEngine, TransactionEngine, TransactionalProducerEngine};

type TransactionEndAdmission<'owner> =
    Result<TransactionEndEngine<'owner>, (TransactionEngine<'owner>, KafkaError)>;

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
fn private_begin_and_end_types_preserve_the_owner_borrow() {
    fn require_begin(
        _method: for<'owner> fn(
            &'owner mut TransactionalProducerEngine,
        ) -> Result<TransactionEngine<'owner>, KafkaError>,
    ) {
    }
    fn require_end_future<F: Future<Output = Result<(), KafkaError>>>() {}
    fn require_commit<'owner>(
        _method: fn(TransactionEngine<'owner>, Option<Instant>) -> TransactionEndAdmission<'owner>,
    ) {
    }
    fn require_abort<'owner>(
        _method: fn(TransactionEngine<'owner>, Option<Instant>) -> TransactionEndAdmission<'owner>,
    ) {
    }

    require_begin(TransactionalProducerEngine::begin);
    require_commit(TransactionEngine::commit);
    require_abort(TransactionEngine::abort);
    require_end_future::<TransactionEndEngine<'static>>();
    assert_not_impl!(TransactionEngine<'static>: Clone);
    assert_not_impl!(TransactionEngine<'static>: Copy);
    assert_not_impl!(TransactionEndEngine<'static>: Clone);
    assert_not_impl!(TransactionEndEngine<'static>: Copy);
}
