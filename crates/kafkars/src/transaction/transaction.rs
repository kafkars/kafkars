//! Linear active transaction borrowing one initialized producer.

use std::time::Duration;

use crate::{Checkpoint, GroupMetadata, Record, bridge::transaction::TransactionEngine};

use super::{
    AbortTransaction, CommitTransaction, SendTransactionBatch, SendTransactionOffsets,
    SendTransactionRecord, TransactionBatchSendAdmissionError, TransactionEndAdmissionError,
    TransactionOffsetsAdmissionError, TransactionSendAdmissionError,
};

/// Opaque active transaction that exclusively borrows its producer.
///
/// Dropping an active transaction does not report success. The engine treats
/// token loss as owner loss and schedules a bounded best-effort abort.
#[derive(Debug)]
#[must_use = "commit, abort, or drop the active transaction"]
pub struct Transaction<'producer> {
    inner: TransactionEngine<'producer>,
}

impl<'producer> Transaction<'producer> {
    pub(crate) const fn from_bridge(inner: TransactionEngine<'producer>) -> Self {
        Self { inner }
    }

    /// Reports an advisory reactor-wake failure after accepted begin.
    pub const fn begin_wake_failed(&self) -> bool {
        self.inner.begin_wake_failed()
    }

    /// Attempts to admit one record into this transaction.
    ///
    /// On rejection, [`TransactionSendAdmissionError`] returns the original
    /// record. On acceptance, the named observer exclusively reborrows this
    /// transaction until that observer is consumed or dropped. An explicit
    /// partition bypasses metadata lookup; otherwise the producer's keyed or
    /// sticky partition policy selects the route under this call's deadline.
    #[expect(
        clippy::result_large_err,
        reason = "pre-admission rejection returns the exact bytes-native record"
    )]
    pub fn send<'send>(
        &'send mut self,
        record: Record,
        timeout: Duration,
    ) -> Result<SendTransactionRecord<'send, 'producer>, TransactionSendAdmissionError> {
        self.inner
            .send(record, timeout)
            .map(SendTransactionRecord::from_bridge)
            .map_err(|(record, error)| TransactionSendAdmissionError::new(record, error))
    }

    /// Attempts to admit one homogeneous record batch into this transaction.
    ///
    /// The batch must be nonempty and every record must use the same topic and
    /// same explicit partition. Rejection returns every original record in
    /// caller order with its shared byte and source ownership intact.
    /// Acceptance reserves one terminal, one sequence range, and one Produce
    /// certainty for the whole batch under the deadline captured at this call.
    pub fn send_batch<'send>(
        &'send mut self,
        records: Vec<Record>,
        timeout: Duration,
    ) -> Result<SendTransactionBatch<'send, 'producer>, TransactionBatchSendAdmissionError> {
        self.inner
            .send_batch(records, timeout)
            .map(SendTransactionBatch::from_bridge)
            .map_err(|(records, error)| TransactionBatchSendAdmissionError::new(records, error))
    }

    /// Attempts to transfer one assignment-fenced checkpoint into this transaction.
    ///
    /// Rejection returns both exact inputs. Acceptance exclusively reborrows
    /// this transaction until the named terminal observer is consumed or dropped.
    #[expect(
        clippy::result_large_err,
        reason = "pre-admission rejection returns the exact group metadata and assignment-fenced checkpoint"
    )]
    pub fn send_offsets<'send>(
        &'send mut self,
        metadata: GroupMetadata,
        checkpoint: Checkpoint,
        timeout: Duration,
    ) -> Result<SendTransactionOffsets<'send, 'producer>, TransactionOffsetsAdmissionError> {
        self.inner
            .send_offsets(metadata, checkpoint, timeout)
            .map(SendTransactionOffsets::from_bridge)
            .map_err(|(metadata, checkpoint, error)| {
                TransactionOffsetsAdmissionError::new(metadata, checkpoint, error)
            })
    }

    /// Attempts to commit this exact active transaction.
    ///
    /// Rejection returns [`TransactionEndAdmissionError`] containing this same
    /// transaction for retry or abort.
    pub fn commit(
        self,
        timeout: Duration,
    ) -> Result<CommitTransaction<'producer>, TransactionEndAdmissionError<'producer>> {
        self.inner
            .commit(timeout)
            .map(CommitTransaction::from_bridge)
            .map_err(|(transaction, error)| {
                TransactionEndAdmissionError::new(Self::from_bridge(transaction), error)
            })
    }

    /// Attempts to abort this exact active transaction.
    ///
    /// Rejection returns [`TransactionEndAdmissionError`] containing this same
    /// transaction for retry or another abort attempt.
    pub fn abort(
        self,
        timeout: Duration,
    ) -> Result<AbortTransaction<'producer>, TransactionEndAdmissionError<'producer>> {
        self.inner
            .abort(timeout)
            .map(AbortTransaction::from_bridge)
            .map_err(|(transaction, error)| {
                TransactionEndAdmissionError::new(Self::from_bridge(transaction), error)
            })
    }
}
