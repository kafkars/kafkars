//! Retained end-admission error public recovery signatures.

use crate::KafkaError;

use super::{Transaction, TransactionEndAdmissionError};

#[test]
fn end_admission_error_recovers_transaction_or_both_parts() {
    fn require_error<'producer>(
        _method: for<'borrow> fn(
            &'borrow TransactionEndAdmissionError<'producer>,
        ) -> &'borrow KafkaError,
    ) {
    }
    fn require_transaction<'producer>(
        _method: fn(TransactionEndAdmissionError<'producer>) -> Transaction<'producer>,
    ) {
    }
    fn require_parts<'producer>(
        _method: fn(
            TransactionEndAdmissionError<'producer>,
        ) -> (Transaction<'producer>, KafkaError),
    ) {
    }

    require_error(TransactionEndAdmissionError::error);
    require_transaction(TransactionEndAdmissionError::into_transaction);
    require_parts(TransactionEndAdmissionError::into_parts);
}
