//! Public Admin `ListOffsetsResultInfo` value scenarios.

use super::ListOffsetsResultInfo;

#[test]
fn result_preserves_offset_timestamp_and_optional_epoch() {
    let value = ListOffsetsResultInfo::new(Some(91), Some(1_700_000_000_123), Some(7));

    assert_eq!(value.offset(), Some(91));
    assert_eq!(value.timestamp_ms(), Some(1_700_000_000_123));
    assert_eq!(value.leader_epoch(), Some(7));
}
