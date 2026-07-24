//! Direct-consumer scalar validation scenarios.

use super::{AssignmentEpoch, FetchRevision, NextFetchOffset, PositionEpoch};

#[test]
fn epochs_are_nonzero_and_offsets_reject_kafka_sentinels() {
    assert_eq!(NextFetchOffset::try_from_raw(-1), None);

    assert_eq!(AssignmentEpoch::initial().get(), 1);
    assert_eq!(PositionEpoch::initial().get(), 1);
    assert_eq!(FetchRevision::initial().get(), 1);
    assert_eq!(FetchRevision::try_from_raw_for_test(0), None);
    assert_eq!(
        NextFetchOffset::try_from_raw(0).map(NextFetchOffset::get),
        Some(0)
    );
}
