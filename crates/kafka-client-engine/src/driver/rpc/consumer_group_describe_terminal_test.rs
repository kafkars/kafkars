//! Local API/version fallback classification scenarios.

use kafka_driver::{ApiVersion, RequestError};
use kafka_wire_core::ApiKey;

use super::consumer_group_describe_terminal::{
    ConsumerGroupDescribeDriverFailureKind, consumer_group_describe_failure_kind,
};

#[test]
fn local_api_and_version_failures_are_explicit_fallback_signals() {
    let unavailable = RequestError::ApiUnavailable {
        api_key: ApiKey::new(69),
    };
    let kind = consumer_group_describe_failure_kind(&unavailable);
    assert_eq!(
        kind,
        ConsumerGroupDescribeDriverFailureKind::LocalApiUnavailable
    );

    let unsupported = RequestError::UnsupportedVersion {
        message: "ConsumerGroupDescribeResponse",
        version: ApiVersion::new(2),
    };
    let kind = consumer_group_describe_failure_kind(&unsupported);
    assert_eq!(
        kind,
        ConsumerGroupDescribeDriverFailureKind::LocalUnsupportedVersion
    );
}
