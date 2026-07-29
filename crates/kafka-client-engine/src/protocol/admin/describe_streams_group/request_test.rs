//! Singleton request shape, version intent, and retained-capacity scenarios.

use super::{DescribeStreamsGroupRequestFailure, describe_streams_group_request};

#[test]
fn request_preserves_single_group_and_optional_expansion_intent() {
    let request = describe_streams_group_request("streams-app", true, true, 4096)
        .unwrap_or_else(|error| panic!("request: {error:?}"));
    assert_eq!(request.group_ids.len(), 1);
    assert_eq!(request.group_ids[0].as_str(), "streams-app");
    assert!(request.include_authorized_operations);
    assert!(request.include_topology_description);
}

#[test]
fn request_rejects_invalid_identity_and_unreserved_storage() {
    assert_eq!(
        describe_streams_group_request("", false, false, 4096),
        Err(DescribeStreamsGroupRequestFailure::EmptyGroupId)
    );
    assert_eq!(
        describe_streams_group_request("streams-app", false, false, 1),
        Err(DescribeStreamsGroupRequestFailure::RetainedBytes)
    );
}
