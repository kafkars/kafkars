//! Unfiltered request compatibility scenarios.

use super::list_consumer_groups_request;

#[test]
fn request_uses_no_broker_side_filters_across_old_and_new_versions() {
    let request = list_consumer_groups_request();
    assert!(request.states_filter.is_empty());
    assert!(request.types_filter.is_empty());
}
