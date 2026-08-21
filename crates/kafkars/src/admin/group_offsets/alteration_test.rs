//! Public committed-offset alteration value scenarios.

use super::ConsumerGroupOffsetAlteration;

#[test]
fn offset_epoch_and_nullable_metadata_remain_exact_and_inert() {
    let alteration = ConsumerGroupOffsetAlteration::new("orders", 7, 42)
        .leader_epoch(9)
        .metadata("");
    assert_eq!(alteration.topic(), "orders");
    assert_eq!(alteration.partition(), 7);
    assert_eq!(alteration.next_offset(), 42);
    assert_eq!(alteration.requested_leader_epoch(), Some(9));
    assert_eq!(alteration.requested_metadata(), Some(""));
    assert_eq!(
        alteration.into_parts(),
        ("orders".to_owned(), 7, 42, Some(9), Some(String::new()))
    );

    let nullable = ConsumerGroupOffsetAlteration::new("audit", 1, 0);
    assert_eq!(nullable.requested_leader_epoch(), None);
    assert_eq!(nullable.requested_metadata(), None);
}
