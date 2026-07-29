//! General group scalar and exact broker-error accessor evidence.

use super::{GroupListing, ListGroupsBrokerError};

#[test]
fn listing_preserves_all_group_type_fields_without_consumer_narrowing() {
    let listing = GroupListing::new(
        "streams-alpha".to_owned(),
        "streams".to_owned(),
        Some("Stable".to_owned()),
        Some("streams".to_owned()),
    );
    assert_eq!(listing.group_id(), "streams-alpha");
    assert_eq!(listing.protocol_type(), "streams");
    assert_eq!(listing.group_state(), Some("Stable"));
    assert_eq!(listing.group_type(), Some("streams"));

    let error = ListGroupsBrokerError::new(7, -17);
    assert_eq!(error.broker_id(), 7);
    assert_eq!(error.code(), -17);
}

#[test]
fn listing_preserves_absent_version_gated_fields() {
    let listing = GroupListing::new("legacy".to_owned(), String::new(), None, None);
    assert_eq!(listing.group_state(), None);
    assert_eq!(listing.group_type(), None);
}
