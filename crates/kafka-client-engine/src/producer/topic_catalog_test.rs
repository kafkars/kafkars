//! Active topic names share identities and prune without identity reuse.

use std::sync::Arc;

use super::topic_catalog::TopicCatalog;

#[test]
fn equal_active_names_share_one_identity_and_reference_count() {
    let mut catalog = TopicCatalog::new();
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
fn pruned_name_receives_a_fresh_identity_and_names_never_alias() {
    let mut catalog = TopicCatalog::new();
    let orders = catalog
        .acquire(Arc::from("orders"))
        .unwrap_or_else(|error| panic!("orders topic failed: {error}"));
    let payments = catalog
        .acquire(Arc::from("payments"))
        .unwrap_or_else(|error| panic!("payments topic failed: {error}"));
    assert_ne!(orders, payments);
    assert_eq!(catalog.name(orders).map(AsRef::as_ref), Ok("orders"));
    assert_eq!(catalog.release(orders), Ok(()));
    let replacement = catalog
        .acquire(Arc::from("orders"))
        .unwrap_or_else(|error| panic!("replacement topic failed: {error}"));

    assert!(replacement.get() > payments.get());
    assert_ne!(replacement, orders);
}
