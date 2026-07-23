//! Pre-core invariant error translation and record-ownership scenarios.

use std::sync::Arc;

use bytes::Bytes;
use kafka_client_core::PartitionIndex;

use super::{ProducerTrySendError, ProducerTrySendErrorKind};
use crate::producer::{
    ProducerHostInvariantError,
    ingress::{ProducerPortAdmissionError, ProducerPortPoison},
    record::ProducerRecord as StoredProducerRecord,
};

#[test]
fn recovered_pre_core_record_survives_poison_translation() {
    let error = ProducerTrySendError::from_port(ProducerPortAdmissionError::Poisoned(
        ProducerPortPoison::BeforeOwnership {
            error: ProducerHostInvariantError::MissingAdmissionIdentity,
            record: stored_record(),
        },
    ));

    assert_eq!(error.kind(), ProducerTrySendErrorKind::InternalInvariant);
    assert_eq!(
        error.detail(),
        Some("accepted producer transition omitted its operation identity")
    );
    let record = error.into_record();
    assert_eq!(record.topic(), "orders");
    assert_eq!(record.explicit_partition(), Some(3));
    assert_eq!(record.timestamp(), Some(7));
    assert_eq!(record.value_bytes(), Some(&Bytes::from_static(b"value")));
}

#[test]
fn every_pre_ownership_poison_retains_the_exact_record() {
    let value = Bytes::from_static(b"still-owned");
    let error = ProducerTrySendError::from_port(ProducerPortAdmissionError::Poisoned(
        ProducerPortPoison::BeforeOwnership {
            error: ProducerHostInvariantError::MissingAdmissionIdentity,
            record: StoredProducerRecord::new(
                Arc::from("audit"),
                PartitionIndex::from_raw(9),
                11,
                None,
                Some(value.clone()),
            ),
        },
    ));

    assert_eq!(error.kind(), ProducerTrySendErrorKind::InternalInvariant);
    assert!(error.detail().is_some());
    let record = error.into_record();
    assert_eq!(record.topic(), "audit");
    assert_eq!(record.explicit_partition(), Some(9));
    assert_eq!(
        record.value_bytes().map(|bytes| bytes.as_ptr()),
        Some(value.as_ptr())
    );
}

fn stored_record() -> StoredProducerRecord {
    StoredProducerRecord::new(
        Arc::from("orders"),
        PartitionIndex::from_raw(3),
        7,
        None,
        Some(Bytes::from_static(b"value")),
    )
}
