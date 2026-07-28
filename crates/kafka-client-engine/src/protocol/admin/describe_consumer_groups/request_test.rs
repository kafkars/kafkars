//! Singleton `DescribeGroups` request construction scenarios.

use super::describe_consumer_group_request;

#[test]
fn request_names_exactly_one_group_and_preserves_authorization_intent() {
    let request = describe_consumer_group_request("workers", true);
    assert_eq!(request.groups.len(), 1);
    assert_eq!(request.groups[0].as_str(), "workers");
    assert!(request.include_authorized_operations);
}
