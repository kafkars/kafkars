//! Direct-consumer scalar validation scenarios.

use super::{AssignmentEpoch, FetchRevision, NextFetchOffset, PositionEpoch};

#[test]
fn epochs_are_nonzero_and_offsets_reject_kafka_sentinels() {
    assert_eq!(AssignmentEpoch::try_from_raw(0), None);
    assert_eq!(PositionEpoch::try_from_raw(0), None);
    assert_eq!(FetchRevision::try_from_raw(0), None);
    assert_eq!(NextFetchOffset::try_from_raw(-1), None);

    assert_eq!(
        AssignmentEpoch::try_from_raw(7).map(AssignmentEpoch::get),
        Some(7)
    );
    assert_eq!(
        PositionEpoch::try_from_raw(8).map(PositionEpoch::get),
        Some(8)
    );
    assert_eq!(
        FetchRevision::try_from_raw(9).map(FetchRevision::get),
        Some(9)
    );
    assert_eq!(
        NextFetchOffset::try_from_raw(0).map(NextFetchOffset::get),
        Some(0)
    );
}
