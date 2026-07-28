//! Atomic topic staging, stable identity, and bounded-retention tests.

use std::sync::Arc;

use kafka_client_core::TopicId;

use super::topic_catalog::{TransactionTopicCatalog, TransactionTopicCatalogError};

#[test]
fn dropped_staging_spends_neither_identity_nor_retained_bytes() {
    let mut catalog = TransactionTopicCatalog::new(2, 32);
    let orders = Arc::<str>::from("orders");
    let staged = catalog
        .prepare(&orders)
        .unwrap_or_else(|error| panic!("first topic stages: {error:?}"));
    assert_eq!(staged.topic_id(), TopicId::from_raw(1));
    drop(staged);
    assert_eq!(catalog.retained_topic_count(), 0);
    assert_eq!(catalog.retained_topic_bytes(), 0);

    let staged = catalog
        .prepare(&orders)
        .unwrap_or_else(|error| panic!("same identity remains available: {error:?}"));
    assert_eq!(staged.topic_id(), TopicId::from_raw(1));
    catalog.commit(staged);
    let existing = catalog
        .prepare(&orders)
        .unwrap_or_else(|error| panic!("committed topic resolves: {error:?}"));
    assert_eq!(existing.topic_id(), TopicId::from_raw(1));
    catalog.commit(existing);

    let payments = Arc::<str>::from("payments");
    let staged = catalog
        .prepare(&payments)
        .unwrap_or_else(|error| panic!("second topic stages: {error:?}"));
    assert_eq!(staged.topic_id(), TopicId::from_raw(2));
    catalog.commit(staged);
    assert_eq!(catalog.retained_topic_count(), 2);
    assert_eq!(
        catalog.retained_topic_bytes(),
        "orders".len() + "payments".len()
    );
}

#[test]
fn topic_count_and_bytes_fail_without_mutating_committed_state() {
    let mut count_catalog = TransactionTopicCatalog::new(1, 32);
    commit(&mut count_catalog, "orders");
    let Err(error) = count_catalog.prepare(&Arc::<str>::from("payments")) else {
        panic!("topic beyond the catalog count bound was unexpectedly prepared");
    };
    assert_eq!(
        error,
        TransactionTopicCatalogError::RetainedTopicCapacity {
            actual: 2,
            limit: 1,
        }
    );
    assert_eq!(count_catalog.retained_topic_count(), 1);
    assert_eq!(count_catalog.retained_topic_bytes(), "orders".len());

    let mut byte_catalog = TransactionTopicCatalog::new(2, "orders".len());
    commit(&mut byte_catalog, "orders");
    let Err(error) = byte_catalog.prepare(&Arc::<str>::from("x")) else {
        panic!("topic beyond the canonical-byte bound was unexpectedly prepared");
    };
    assert_eq!(
        error,
        TransactionTopicCatalogError::RetainedTopicBytes {
            actual: "orders".len() + 1,
            limit: "orders".len(),
        }
    );
    assert_eq!(byte_catalog.retained_topic_count(), 1);
    assert_eq!(byte_catalog.retained_topic_bytes(), "orders".len());
}

#[test]
fn exhausted_cursor_rejects_only_new_topics() {
    let mut catalog = TransactionTopicCatalog::new(2, 32);
    commit(&mut catalog, "orders");
    catalog.set_next_topic_id(None);

    let Err(error) = catalog.prepare(&Arc::<str>::from("payments")) else {
        panic!("new topic was unexpectedly prepared after identity exhaustion");
    };
    assert_eq!(error, TransactionTopicCatalogError::TopicIdentityExhausted);
    let existing = catalog
        .prepare(&Arc::<str>::from("orders"))
        .unwrap_or_else(|error| {
            panic!("existing producer-lifetime identity remains stable: {error:?}")
        });
    assert_eq!(existing.topic_id(), TopicId::from_raw(1));
}

fn commit(catalog: &mut TransactionTopicCatalog, topic: &str) {
    let prepared = catalog
        .prepare(&Arc::<str>::from(topic))
        .unwrap_or_else(|error| panic!("topic stages: {error:?}"));
    catalog.commit(prepared);
}
