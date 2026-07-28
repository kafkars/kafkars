//! Stable producer batch outcome ownership scenarios.

use super::super::{
    ProducerTrySendBatchError, ProducerTrySendError, ProducerTrySendErrorKind,
    PublicProducerRecord as ProducerRecord,
};

#[test]
fn rejected_suffix_keeps_first_record_before_untouched_records() {
    let error = ProducerTrySendError::with_record(
        ProducerTrySendErrorKind::CompletionCapacity,
        ProducerRecord::to("first").partition(0),
    );
    let batch = ProducerTrySendBatchError::from_single(
        error,
        vec![ProducerRecord::to("second").partition(0)],
    );

    assert_eq!(
        batch
            .records()
            .iter()
            .map(ProducerRecord::topic)
            .collect::<Vec<_>>(),
        vec!["first", "second"]
    );
}
