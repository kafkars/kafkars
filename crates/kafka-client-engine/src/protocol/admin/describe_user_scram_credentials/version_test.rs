//! Exact generated version-window evidence for API-key 50.

use kafka_wire::{DescribeUserScramCredentialsRequest, KafkaMessage};
use kafka_wire_core::ApiVersion;

use super::version::{
    DESCRIBE_USER_SCRAM_CREDENTIALS_MAX_VERSION, DESCRIBE_USER_SCRAM_CREDENTIALS_MIN_VERSION,
    supports_describe_user_scram_credentials_version,
};

#[test]
fn seam_matches_the_generated_v0_only_window() {
    assert_eq!(DESCRIBE_USER_SCRAM_CREDENTIALS_MIN_VERSION, 0);
    assert_eq!(DESCRIBE_USER_SCRAM_CREDENTIALS_MAX_VERSION, 0);
    assert!(!supports_describe_user_scram_credentials_version(-1));
    assert!(supports_describe_user_scram_credentials_version(0));
    assert!(!supports_describe_user_scram_credentials_version(1));
    assert!(DescribeUserScramCredentialsRequest::SUPPORTED_VERSIONS.contains(ApiVersion::new(0)));
    assert!(!DescribeUserScramCredentialsRequest::SUPPORTED_VERSIONS.contains(ApiVersion::new(1)));
}
