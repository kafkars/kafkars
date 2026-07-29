//! Stable ShareGroup offset value tests.

use super::ShareGroupOffset;

#[test]
fn accessors_preserve_every_stable_field() {
    let topic_id = [9; 16];
    let offset = ShareGroupOffset::new(topic_id, Some(41), Some(7), Some(13));

    assert_eq!(offset.topic_id(), topic_id);
    assert_eq!(offset.start_offset(), Some(41));
    assert_eq!(offset.leader_epoch(), Some(7));
    assert_eq!(offset.lag(), Some(13));
}

#[test]
fn optional_values_remain_absent() {
    let offset = ShareGroupOffset::new([1; 16], None, None, None);

    assert_eq!(offset.start_offset(), None);
    assert_eq!(offset.leader_epoch(), None);
    assert_eq!(offset.lag(), None);
}
