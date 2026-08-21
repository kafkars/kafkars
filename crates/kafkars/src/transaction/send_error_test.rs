//! Transactional-send admission-error recovery signatures.

use crate::{KafkaError, Record};

use super::TransactionSendAdmissionError;

#[test]
fn send_admission_error_recovers_exact_record_or_both_parts() {
    fn require_error(
        _method: for<'borrow> fn(&'borrow TransactionSendAdmissionError) -> &'borrow KafkaError,
    ) {
    }
    fn require_record(_method: fn(TransactionSendAdmissionError) -> Record) {}
    fn require_parts(_method: fn(TransactionSendAdmissionError) -> (Record, KafkaError)) {}

    require_error(TransactionSendAdmissionError::error);
    require_record(TransactionSendAdmissionError::into_record);
    require_parts(TransactionSendAdmissionError::into_parts);
}
