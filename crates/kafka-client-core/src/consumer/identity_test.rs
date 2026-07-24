//! Direct-consumer identity and generation validation scenarios.

use super::{AssignmentEpoch, FetchRevision, PositionEpoch};

#[test]
fn lifecycle_generations_begin_nonzero() {
    assert_eq!(AssignmentEpoch::initial().get(), 1);
    assert_eq!(PositionEpoch::initial().get(), 1);
    assert_eq!(FetchRevision::initial().get(), 1);
    assert_eq!(FetchRevision::try_from_raw_for_test(0), None);
}
