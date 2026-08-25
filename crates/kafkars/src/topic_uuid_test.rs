//! Public nonzero topic-UUID value tests.

use super::TopicUuid;

#[test]
fn topic_uuid_rejects_zero_and_preserves_exact_bytes() {
    assert_eq!(TopicUuid::try_from_bytes([0; 16]), None);

    let bytes = [0x5a; 16];
    let uuid = TopicUuid::try_from_bytes(bytes).unwrap_or_else(|| panic!("nonzero topic UUID"));
    assert_eq!(uuid.as_bytes(), &bytes);
    assert_eq!(uuid.into_bytes(), bytes);
}
