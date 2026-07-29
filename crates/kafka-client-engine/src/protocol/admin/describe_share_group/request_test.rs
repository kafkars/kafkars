//! Singleton exact-v1 request shape and retained-capacity scenarios.

use super::{DescribeShareGroupRequestFailure, describe_share_group_request};

#[test]
fn request_preserves_single_group_and_authorized_operations_intent() {
    let request = describe_share_group_request("share-readers", true, 4096)
        .unwrap_or_else(|error| panic!("request: {error:?}"));
    assert_eq!(request.group_ids.len(), 1);
    assert_eq!(request.group_ids[0].as_str(), "share-readers");
    assert!(request.include_authorized_operations);
}

#[test]
fn request_rejects_invalid_identity_and_unreserved_storage() {
    assert_eq!(
        describe_share_group_request("", false, 4096),
        Err(DescribeShareGroupRequestFailure::EmptyGroupId)
    );
    assert_eq!(
        describe_share_group_request("share-readers", false, 1),
        Err(DescribeShareGroupRequestFailure::RetainedBytes)
    );
}
