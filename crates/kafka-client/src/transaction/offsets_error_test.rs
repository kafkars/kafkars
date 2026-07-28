//! Public exact-input recovery contract for rejected transactional offsets.

use crate::{Checkpoint, GroupMetadata, KafkaError};

use super::TransactionOffsetsAdmissionError;

#[test]
fn rejection_exposes_borrowed_and_owned_exact_inputs() {
    fn require_error(_method: fn(&TransactionOffsetsAdmissionError) -> &KafkaError) {}
    fn require_metadata(_method: fn(&TransactionOffsetsAdmissionError) -> &GroupMetadata) {}
    fn require_checkpoint(_method: fn(&TransactionOffsetsAdmissionError) -> &Checkpoint) {}
    fn require_parts(
        _method: fn(TransactionOffsetsAdmissionError) -> (GroupMetadata, Checkpoint, KafkaError),
    ) {
    }

    require_error(TransactionOffsetsAdmissionError::error);
    require_metadata(TransactionOffsetsAdmissionError::metadata);
    require_checkpoint(TransactionOffsetsAdmissionError::checkpoint);
    require_parts(TransactionOffsetsAdmissionError::into_parts);
}
