//! Exact whole-vector transactional batch admission recovery.

use bytes::Bytes;

use crate::{ErrorKind, KafkaError, Record};

use super::TransactionBatchSendAdmissionError;

#[test]
fn rejection_recovers_every_original_record_in_order() {
    let records = vec![
        Record::to("orders")
            .partition(2)
            .value(Bytes::from_static(b"first")),
        Record::to("orders")
            .partition(2)
            .value(Bytes::from_static(b"second")),
    ];
    let rejection = TransactionBatchSendAdmissionError::new(
        records,
        KafkaError::new(ErrorKind::InvalidRecord, "mixed batch"),
    );

    assert_eq!(rejection.records().len(), 2);
    let (records, error) = rejection.into_parts();
    assert_eq!(error.kind(), ErrorKind::InvalidRecord);
    assert_eq!(
        records[0].value_bytes(),
        Some(&Bytes::from_static(b"first"))
    );
    assert_eq!(
        records[1].value_bytes(),
        Some(&Bytes::from_static(b"second"))
    );
}
