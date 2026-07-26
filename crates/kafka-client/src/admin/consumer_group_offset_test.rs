//! Public committed consumer-group offset value scenarios.

use super::ConsumerGroupOffset;

#[test]
fn optional_offset_epoch_and_nullable_metadata_remain_distinct() {
    let present = ConsumerGroupOffset::new(Some(42), Some(7), Some("processed".to_owned()));
    assert_eq!(present.committed_offset(), Some(42));
    assert_eq!(present.leader_epoch(), Some(7));
    assert_eq!(present.metadata(), Some("processed"));
    assert_eq!(
        present.into_parts(),
        (Some(42), Some(7), Some("processed".to_owned()))
    );

    let missing = ConsumerGroupOffset::new(None, None, None);
    assert_eq!(missing.committed_offset(), None);
    assert_eq!(missing.leader_epoch(), None);
    assert_eq!(missing.metadata(), None);
}
