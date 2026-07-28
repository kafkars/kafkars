//! Active references and producer-lifetime stable topic identities.

use std::sync::Arc;

use super::topic_catalog::TopicCatalog;

#[test]
fn equal_active_names_share_one_identity_and_reference_count() {
    let mut catalog = TopicCatalog::new(2, 64);
    let first = catalog
        .acquire(Arc::from("orders"))
        .unwrap_or_else(|error| panic!("first topic failed: {error}"));
    let second = catalog
        .acquire(Arc::from("orders"))
        .unwrap_or_else(|error| panic!("second topic failed: {error}"));

    assert_eq!(first, second);
    assert_eq!(catalog.len(), 1);
    assert_eq!(catalog.release(first), Ok(()));
    assert_eq!(catalog.len(), 1);
    assert_eq!(catalog.release(second), Ok(()));
    assert_eq!(catalog.len(), 0);
}

#[test]
fn inactive_name_retains_its_identity_and_names_never_alias() {
    let mut catalog = TopicCatalog::new(2, 64);
    let orders = catalog
        .acquire(Arc::from("orders"))
        .unwrap_or_else(|error| panic!("orders topic failed: {error}"));
    let payments = catalog
        .acquire(Arc::from("payments"))
        .unwrap_or_else(|error| panic!("payments topic failed: {error}"));
    assert_ne!(orders, payments);
    assert_eq!(catalog.name(orders).map(AsRef::as_ref), Ok("orders"));
    assert_eq!(catalog.release(orders), Ok(()));
    assert_eq!(catalog.len(), 1);
    let replacement = catalog
        .acquire(Arc::from("orders"))
        .unwrap_or_else(|error| panic!("replacement topic failed: {error}"));

    assert_eq!(replacement, orders);
    assert_ne!(replacement, payments);
    assert_eq!(catalog.release(replacement), Ok(()));
    assert_eq!(catalog.release(payments), Ok(()));
    assert_eq!(catalog.len(), 0);
    catalog.clear_terminal();
    assert_eq!(
        catalog.name(orders),
        Err(super::ProducerStoreError::UnknownTopic)
    );
}

#[test]
fn historical_capacity_exhaustion_is_not_active_backpressure() {
    let mut count_catalog = TopicCatalog::new(1, 64);
    let orders = count_catalog
        .acquire(Arc::from("orders"))
        .unwrap_or_else(|error| panic!("orders topic failed: {error}"));
    assert_eq!(count_catalog.release(orders), Ok(()));
    assert_eq!(count_catalog.len(), 0);
    assert_eq!(
        count_catalog.acquire(Arc::from("payments")),
        Err(super::ProducerStoreError::TopicIdentityExhausted)
    );
    assert_eq!(count_catalog.len(), 0);

    let mut byte_catalog = TopicCatalog::new(2, 6);
    let orders = byte_catalog
        .acquire(Arc::from("orders"))
        .unwrap_or_else(|error| panic!("bounded orders topic failed: {error}"));
    assert_eq!(byte_catalog.release(orders), Ok(()));
    assert_eq!(
        byte_catalog.acquire(Arc::from("x")),
        Err(super::ProducerStoreError::TopicIdentityExhausted)
    );
    assert_eq!(byte_catalog.len(), 0);
}
