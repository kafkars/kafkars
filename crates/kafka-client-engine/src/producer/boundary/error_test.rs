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
            record: Some(stored_record()),
        },
    ));

    assert_eq!(error.kind(), ProducerTrySendErrorKind::InternalInvariant);
    assert_eq!(
        error.detail(),
        Some("accepted producer transition omitted its operation identity")
    );
    let Some(record) = error.into_record() else {
        panic!("successful rollback must retain the exact public record");
    };
    assert_eq!(record.topic(), "orders");
    assert_eq!(record.explicit_partition(), Some(3));
    assert_eq!(record.timestamp(), Some(7));
    assert_eq!(record.value_bytes(), Some(&Bytes::from_static(b"value")));
}

#[test]
fn corrupted_record_store_is_the_only_recordless_poison_shape() {
    let error = ProducerTrySendError::from_port(ProducerPortAdmissionError::Poisoned(
        ProducerPortPoison::BeforeOwnership {
            error: ProducerHostInvariantError::MissingAdmissionIdentity,
            record: None,
        },
    ));

    assert_eq!(error.kind(), ProducerTrySendErrorKind::InternalInvariant);
    assert!(error.detail().is_some());
    assert!(error.into_record().is_none());
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
