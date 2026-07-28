//! Singleton API-key 69 request construction scenarios.

use super::{ConsumerGroupDescribeRequestFailure, consumer_group_describe_request};

#[test]
fn request_preserves_exact_group_and_authorization_intent() {
    let request = consumer_group_describe_request("workers", true, 4096)
        .unwrap_or_else(|error| panic!("valid request: {error:?}"));
    assert_eq!(request.group_ids.len(), 1);
    assert_eq!(request.group_ids[0].as_str(), "workers");
    assert!(request.include_authorized_operations);
}

#[test]
fn request_rejects_invalid_identity_and_unreserved_bytes() {
    assert_eq!(
        consumer_group_describe_request("", false, usize::MAX),
        Err(ConsumerGroupDescribeRequestFailure::EmptyGroupId)
    );
    assert_eq!(
        consumer_group_describe_request("workers", false, 1),
        Err(ConsumerGroupDescribeRequestFailure::RetainedBytes)
    );
    let oversized = "g".repeat(i16::MAX as usize + 1);
    assert_eq!(
        consumer_group_describe_request(&oversized, false, usize::MAX),
        Err(ConsumerGroupDescribeRequestFailure::GroupIdTooLong)
    );
}
