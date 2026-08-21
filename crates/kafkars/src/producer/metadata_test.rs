//! Public acknowledged-record metadata accessor scenarios.

use super::RecordMetadata;

#[test]
fn metadata_preserves_every_acknowledgement_field() {
    let metadata = RecordMetadata::from_parts(
        "orders".to_owned(),
        7,
        42,
        Some(1_700_000_000_000),
        Some(3),
        Some(8),
        Some(7),
    );

    assert_eq!(metadata.topic(), "orders");
    assert_eq!(metadata.partition(), 7);
    assert_eq!(metadata.offset(), 42);
    assert_eq!(metadata.timestamp_milliseconds(), Some(1_700_000_000_000));
    assert_eq!(metadata.leader_epoch(), Some(3));
    assert_eq!(metadata.serialized_key_size(), Some(8));
    assert_eq!(metadata.serialized_value_size(), Some(7));
}

#[test]
fn serialized_sizes_distinguish_null_from_present_empty_fields() {
    let null = RecordMetadata::from_parts("orders".to_owned(), 0, 1, None, None, None, None);
    let empty = RecordMetadata::from_parts("orders".to_owned(), 0, 2, None, None, Some(0), Some(0));

    let _: Option<usize> = null.serialized_key_size();
    let _: Option<usize> = null.serialized_value_size();
    assert_eq!(null.serialized_key_size(), None);
    assert_eq!(null.serialized_value_size(), None);
    assert_eq!(empty.serialized_key_size(), Some(0));
    assert_eq!(empty.serialized_value_size(), Some(0));
}
