//! Public Admin `ListOffsetsQuery` value scenarios.

use super::{ListOffsetsQuery, OffsetSpec};

#[test]
fn query_preserves_identity_and_spec_without_early_validation() {
    let query =
        ListOffsetsQuery::new("orders", -1, OffsetSpec::for_timestamp(-1)).current_leader_epoch(-1);

    assert_eq!(query.topic(), "orders");
    assert_eq!(query.partition(), -1);
    assert_eq!(query.spec(), OffsetSpec::Timestamp(-1));
    assert_eq!(query.requested_current_leader_epoch(), Some(-1));
    assert_eq!(
        query.into_parts(),
        ("orders".to_owned(), -1, OffsetSpec::Timestamp(-1), Some(-1))
    );

    assert_eq!(
        ListOffsetsQuery::new("audit", 0, OffsetSpec::latest()).requested_current_leader_epoch(),
        None
    );
}
