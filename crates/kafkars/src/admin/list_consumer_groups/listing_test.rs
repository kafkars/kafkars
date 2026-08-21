//! Public scalar and exact broker-error accessors.

use super::{ConsumerGroupListing, ListConsumerGroupsBrokerError};

#[test]
fn listing_preserves_old_and_new_optional_fields() {
    let listing = ConsumerGroupListing::new(
        "alpha".to_owned(),
        "consumer".to_owned(),
        Some("Stable".to_owned()),
        Some("classic".to_owned()),
    );
    assert_eq!(listing.group_id(), "alpha");
    assert_eq!(listing.protocol_type(), "consumer");
    assert_eq!(listing.group_state(), Some("Stable"));
    assert_eq!(listing.group_type(), Some("classic"));

    let error = ListConsumerGroupsBrokerError::new(7, -17);
    assert_eq!(error.broker_id(), 7);
    assert_eq!(error.code(), -17);
}
