//! Public acknowledged-record metadata accessor scenarios.

use super::RecordMetadata;

#[test]
fn metadata_preserves_every_acknowledgement_field() {
    let metadata =
        RecordMetadata::from_parts("orders".to_owned(), 7, 42, Some(1_700_000_000_000), Some(3));

    assert_eq!(metadata.topic(), "orders");
    assert_eq!(metadata.partition(), 7);
    assert_eq!(metadata.offset(), 42);
    assert_eq!(metadata.timestamp_milliseconds(), Some(1_700_000_000_000));
    assert_eq!(metadata.leader_epoch(), Some(3));
}
