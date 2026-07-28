//! Active transaction lifetime, linearity, and end-admission signatures.

use std::time::Duration;

use super::{AbortTransaction, CommitTransaction, Transaction, TransactionEndAdmissionError};

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
fn active_transaction_is_linear_and_end_rejection_retains_it() {
    fn require_wake_status(_method: fn(&Transaction<'static>) -> bool) {}
    fn require_commit<'producer>(
        _method: fn(
            Transaction<'producer>,
            Duration,
        ) -> Result<
            CommitTransaction<'producer>,
            TransactionEndAdmissionError<'producer>,
        >,
    ) {
    }
    fn require_abort<'producer>(
        _method: fn(
            Transaction<'producer>,
            Duration,
        )
            -> Result<AbortTransaction<'producer>, TransactionEndAdmissionError<'producer>>,
    ) {
    }

    require_wake_status(Transaction::begin_wake_failed);
    require_commit(Transaction::commit);
    require_abort(Transaction::abort);
    assert_not_impl!(Transaction<'static>: Clone);
    assert_not_impl!(Transaction<'static>: Copy);
}
