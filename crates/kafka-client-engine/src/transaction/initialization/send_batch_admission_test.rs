//! Exact-vector homogeneous transactional batch rejection scenarios.

use bytes::Bytes;

use super::{TransactionBatchSendAdmissionError, TransactionSendAdmissionErrorKind};
use crate::producer::PublicProducerRecord;

#[test]
fn rejection_recovers_the_exact_whole_batch_in_order() {
    let records = vec![
        PublicProducerRecord::to("orders")
            .partition(2)
            .value(Bytes::from_static(b"first")),
        PublicProducerRecord::to("orders")
            .partition(2)
            .value(Bytes::from_static(b"second")),
    ];
    let error = TransactionBatchSendAdmissionError::new(
        TransactionSendAdmissionErrorKind::MixedBatchPartition,
        records,
    );

    assert_eq!(error.records().len(), 2);
    let (kind, records) = error.into_parts();
    assert_eq!(kind, TransactionSendAdmissionErrorKind::MixedBatchPartition);
    assert_eq!(
        records[0].value_bytes(),
        Some(&Bytes::from_static(b"first"))
    );
    assert_eq!(
        records[1].value_bytes(),
        Some(&Bytes::from_static(b"second"))
    );
}
