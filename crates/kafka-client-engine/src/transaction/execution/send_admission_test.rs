//! Batch-count preflight and exact unresolved-input recovery scenarios.

use std::sync::Arc;

use bytes::Bytes;
use kafka_client_core::{CompressionPolicy, PartitionIndex};

use crate::{
    producer::{PublicProducerRecord, materialization::MaterializationRecord},
    transaction::{TransactionExecutionSendAdmissionErrorKind, send::TransactionSendInput},
};

use super::test_support::{Fixture, deadline};

#[test]
fn execution_count_guard_returns_the_exact_whole_batch_before_topic_mutation() {
    let mut fixture = Fixture::with_limits(CompressionPolicy::None, 2, 1_024, 1_024, 1_024);
    let epoch = fixture
        .host
        .begin()
        .unwrap_or_else(|error| panic!("transaction begins: {error:?}"));
    let source = Arc::new(());
    let source_weak = Arc::downgrade(&source);
    let topic = Arc::<str>::from("orders");
    let originals = (0..3)
        .map(|_| {
            PublicProducerRecord::to(Arc::clone(&topic))
                .partition(2)
                .value(Bytes::from_static(b"value"))
                .retain_source_owner(source.clone())
        })
        .collect::<Vec<_>>();
    drop(source);
    let records = (0..3)
        .map(|_| {
            MaterializationRecord::new(1_000, None, Some(Bytes::from_static(b"value")), Vec::new())
        })
        .collect();
    let input = TransactionSendInput::new_batch(
        epoch,
        originals,
        Arc::clone(&topic),
        PartitionIndex::from_raw(2),
        records,
        18,
        deadline(40),
    );

    let Err(error) = fixture.host.try_send(fixture.owner_id, input) else {
        panic!("over-count batch was unexpectedly admitted")
    };
    assert_eq!(
        error.kind(),
        TransactionExecutionSendAdmissionErrorKind::BatchRecordCapacity {
            actual: 3,
            limit: 2,
        }
    );
    assert_eq!(fixture.host.topic_id_for_test("orders"), None);
    assert!(source_weak.upgrade().is_some());
    let originals = error.into_input().into_original_records();
    assert_eq!(originals.len(), 3);
    assert!(source_weak.upgrade().is_some());
    drop(originals);
    assert!(source_weak.upgrade().is_none());
    fixture.shutdown_driver();
}
