//! Direct-consumer value validation scenarios.

use super::NextFetchOffset;

#[test]
fn offsets_reject_kafka_sentinels() {
    assert_eq!(NextFetchOffset::try_from_raw(-1), None);
    assert_eq!(
        NextFetchOffset::try_from_raw(0).map(NextFetchOffset::get),
        Some(0)
    );
}
