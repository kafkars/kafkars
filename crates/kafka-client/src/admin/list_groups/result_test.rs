//! Caller-neutral all-group ordering and partial broker-error evidence.

use std::time::Duration;

use super::{GroupListing, ListGroupsBrokerError, ListGroupsResult};

#[test]
fn result_preserves_owner_supplied_global_order_and_broker_errors() {
    let groups = vec![
        GroupListing::new(
            "consumer-alpha".to_owned(),
            "consumer".to_owned(),
            Some("Stable".to_owned()),
            Some("consumer".to_owned()),
        ),
        GroupListing::new(
            "share-beta".to_owned(),
            "share".to_owned(),
            Some("Stable".to_owned()),
            Some("share".to_owned()),
        ),
        GroupListing::new(
            "streams-gamma".to_owned(),
            "streams".to_owned(),
            Some("Stable".to_owned()),
            Some("streams".to_owned()),
        ),
    ];
    let errors = vec![
        ListGroupsBrokerError::new(2, -14),
        ListGroupsBrokerError::new(8, -32_000),
    ];
    let result = ListGroupsResult::new(Duration::from_millis(79), groups, errors);

    assert_eq!(result.throttle_time(), Duration::from_millis(79));
    assert_eq!(result.groups()[0].group_id(), "consumer-alpha");
    assert_eq!(result.groups()[1].group_type(), Some("share"));
    assert_eq!(result.groups()[2].protocol_type(), "streams");
    assert_eq!(result.broker_errors()[0].broker_id(), 2);
    assert_eq!(result.broker_errors()[1].code(), -32_000);

    let (throttle, groups, errors) = result.into_parts();
    assert_eq!(throttle, Duration::from_millis(79));
    assert_eq!(groups.len(), 3);
    assert_eq!(errors.len(), 2);
}
