//! Linear active transaction borrowing one initialized producer.

use std::time::Duration;

use crate::{Checkpoint, GroupMetadata, Record, bridge::transaction::TransactionEngine};

use super::{
    AbortTransaction, CommitTransaction, SendTransactionBatch, SendTransactionOffsets,
    SendTransactionRecord, TransactionBatchSendAdmissionError, TransactionEndAdmissionError,
    TransactionOffsetsAdmissionError, TransactionSendAdmissionError, ValidateTransaction,
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
    /// partition bypasses metadata lookup unless the record carries an expected
    /// topic UUID; bound records require an exact broker topic view before
    /// enrollment. Otherwise the producer's keyed or sticky partition policy
    /// selects the route under this call's deadline.
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
    /// same explicit partition and the same optional expected topic UUID.
    /// Rejection returns every original record in caller order with its shared
    /// byte and source ownership intact.
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

    /// Validates every UUID-bound topic through one fresh broker metadata view.
    ///
    /// The active transaction must first be quiescent. The deadline starts at
    /// this call boundary, and the result must completely correlate the exact
    /// current set of UUID-bound topics. Success installs a seal for the
    /// transaction's current send/offset revision. Any later accepted send,
    /// batch, or offset transfer invalidates that seal. An identity mismatch
    /// latches abort-required state; dropping the observer never installs a
    /// seal.
    ///
    /// `DescribeTopics`, name-routed Produce, and `EndTxn` are separate Kafka
    /// operations. The deployment must prevent deletion and recreation of each
    /// UUID-bound topic from its first accepted bound send through the terminal
    /// `EndTxn` outcome.
    pub fn validate_for_commit<'validation>(
        &'validation mut self,
        timeout: Duration,
    ) -> Result<ValidateTransaction<'validation, 'producer>, crate::KafkaError> {
        let deadline = validation_deadline_at(std::time::Instant::now(), timeout)?;
        self.inner
            .validate_for_commit(deadline)
            .map(ValidateTransaction::from_bridge)
    }

    /// Attempts to commit this exact active transaction.
    ///
    /// A transaction containing any UUID-bound topic requires a fresh complete
    /// validation seal for its current revision. Missing or stale validation
    /// refuses commit, and a latched mismatch requires abort.
    ///
    /// Rejection returns [`TransactionEndAdmissionError`] containing this same
    /// transaction for retry or abort.
    #[expect(
        clippy::result_large_err,
        reason = "commit rejection returns the exact active transaction owner for retry or abort"
    )]
    pub fn commit(
        self,
        timeout: Duration,
    ) -> Result<CommitTransaction<'producer>, TransactionEndAdmissionError<'producer>> {
        let deadline = end_deadline_at(std::time::Instant::now(), timeout);
        self.inner
            .commit(deadline)
            .map(CommitTransaction::from_bridge)
            .map_err(|(transaction, error)| {
                TransactionEndAdmissionError::new(Self::from_bridge(transaction), error)
            })
    }

    /// Attempts to abort this exact active transaction.
    ///
    /// Abort deliberately bypasses the topic-validation seal and mismatch
    /// latch. It remains the terminal path after identity validation fails.
    ///
    /// Rejection returns [`TransactionEndAdmissionError`] containing this same
    /// transaction for retry or another abort attempt.
    #[expect(
        clippy::result_large_err,
        reason = "abort rejection returns the exact active transaction owner for another attempt"
    )]
    pub fn abort(
        self,
        timeout: Duration,
    ) -> Result<AbortTransaction<'producer>, TransactionEndAdmissionError<'producer>> {
        let deadline = end_deadline_at(std::time::Instant::now(), timeout);
        self.inner
            .abort(deadline)
            .map(AbortTransaction::from_bridge)
            .map_err(|(transaction, error)| {
                TransactionEndAdmissionError::new(Self::from_bridge(transaction), error)
            })
    }
}

pub(super) fn validation_deadline_at(
    boundary: std::time::Instant,
    timeout: Duration,
) -> Result<std::time::Instant, crate::KafkaError> {
    if timeout.is_zero() {
        return Err(crate::KafkaError::new(
            crate::ErrorKind::Timeout,
            "transaction validation deadline elapsed at admission",
        ));
    }
    boundary.checked_add(timeout).ok_or_else(|| {
        crate::KafkaError::new(
            crate::ErrorKind::Timeout,
            "transaction validation deadline cannot be represented",
        )
    })
}

pub(super) fn end_deadline_at(
    boundary: std::time::Instant,
    timeout: Duration,
) -> Option<std::time::Instant> {
    if timeout.is_zero() {
        None
    } else {
        boundary.checked_add(timeout)
    }
}
