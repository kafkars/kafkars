//! Active transaction lifetime, linearity, and end-admission signatures.

use std::time::Duration;

use crate::{Checkpoint, GroupMetadata, Record};

use super::{
    AbortTransaction, CommitTransaction, SendTransactionBatch, SendTransactionOffsets,
    SendTransactionRecord, Transaction, TransactionBatchSendAdmissionError,
    TransactionEndAdmissionError, TransactionOffsetsAdmissionError, TransactionSendAdmissionError,
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
    fn require_send<'send, 'producer>(
        _method: fn(
            &'send mut Transaction<'producer>,
            Record,
            Duration,
        ) -> Result<
            SendTransactionRecord<'send, 'producer>,
            TransactionSendAdmissionError,
        >,
    ) {
    }
    fn require_offsets<'send, 'producer>(
        _method: fn(
            &'send mut Transaction<'producer>,
            GroupMetadata,
            Checkpoint,
            Duration,
        ) -> Result<
            SendTransactionOffsets<'send, 'producer>,
            TransactionOffsetsAdmissionError,
        >,
    ) {
    }
    fn require_batch<'send, 'producer>(
        _method: fn(
            &'send mut Transaction<'producer>,
            Vec<Record>,
            Duration,
        ) -> Result<
            SendTransactionBatch<'send, 'producer>,
            TransactionBatchSendAdmissionError,
        >,
    ) {
    }

    require_wake_status(Transaction::begin_wake_failed);
    require_send(Transaction::send);
    require_batch(Transaction::send_batch);
    require_offsets(Transaction::send_offsets);
    require_commit(Transaction::commit);
    require_abort(Transaction::abort);
    assert_not_impl!(Transaction<'static>: Clone);
    assert_not_impl!(Transaction<'static>: Copy);
}
