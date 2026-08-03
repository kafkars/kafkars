//! Exact outer API windows and bounded inner schema configuration.

use kafka_wire::{JOIN_GROUP_API_DESCRIPTOR, SYNC_GROUP_API_DESCRIPTOR};
use kafka_wire_core::ApiVersion;

use super::validation::{
    COOPERATIVE_SUBSCRIPTION_VERSION, INNER_SCHEMA_VERSION, JOIN_MAX_VERSION, JOIN_MIN_VERSION,
    MAX_COOPERATIVE_SUBSCRIPTION_VERSION, MAX_KAFKA_STRING_BYTES, MAX_MEMBER_PARTITIONS,
    MAX_MEMBERS, MAX_SUBSCRIPTION_PAYLOAD_BYTES, MAX_TOPICS, STATIC_JOIN_VERSION,
    STATIC_SYNC_VERSION, SYNC_MAX_VERSION, SYNC_MIN_VERSION, inner_decode_limits,
    subscription_decode_limits, valid_join_version, valid_sync_version,
};

#[test]
fn outer_windows_match_the_driver_dynamic_and_static_membership_contract() {
    assert_eq!((JOIN_MIN_VERSION, JOIN_MAX_VERSION), (1, 3));
    assert_eq!((SYNC_MIN_VERSION, SYNC_MAX_VERSION), (0, 2));
    assert!(valid_join_version(1));
    assert!(valid_join_version(3));
    assert!(valid_join_version(STATIC_JOIN_VERSION));
    assert!(!valid_join_version(0));
    assert!(!valid_join_version(4));
    assert!(!valid_join_version(6));
    assert!(valid_sync_version(0));
    assert!(valid_sync_version(2));
    assert!(valid_sync_version(STATIC_SYNC_VERSION));
    assert!(!valid_sync_version(-1));
    assert!(!valid_sync_version(4));
    assert!(
        JOIN_GROUP_API_DESCRIPTOR
            .supported_versions
            .contains(ApiVersion::new(JOIN_MIN_VERSION))
    );
    assert!(
        JOIN_GROUP_API_DESCRIPTOR
            .supported_versions
            .contains(ApiVersion::new(JOIN_MAX_VERSION))
    );
    assert!(
        SYNC_GROUP_API_DESCRIPTOR
            .supported_versions
            .contains(ApiVersion::new(SYNC_MIN_VERSION))
    );
    assert!(
        SYNC_GROUP_API_DESCRIPTOR
            .supported_versions
            .contains(ApiVersion::new(SYNC_MAX_VERSION))
    );
    assert!(
        JOIN_GROUP_API_DESCRIPTOR
            .supported_versions
            .contains(ApiVersion::new(STATIC_JOIN_VERSION))
    );
    assert!(
        SYNC_GROUP_API_DESCRIPTOR
            .supported_versions
            .contains(ApiVersion::new(STATIC_SYNC_VERSION))
    );
}

#[test]
fn inner_schemas_and_decode_limits_match_owned_bounds() {
    assert_eq!(INNER_SCHEMA_VERSION, 0);
    assert_eq!(COOPERATIVE_SUBSCRIPTION_VERSION, 2);
    assert_eq!(MAX_COOPERATIVE_SUBSCRIPTION_VERSION, 3);
    let limits = inner_decode_limits();
    assert_eq!(limits.max_array_elements, MAX_TOPICS);
    assert_eq!(MAX_MEMBERS, 64);
    assert_eq!(MAX_TOPICS, 64);
    assert_eq!(MAX_MEMBER_PARTITIONS, 64);
    assert_eq!(limits.max_tagged_fields, 0);
    assert_eq!(limits.max_tag_bytes, 0);
    assert_eq!(limits.max_total_tag_bytes, 0);
    let subscription_limits = subscription_decode_limits();
    assert_eq!(
        subscription_limits.max_array_elements,
        MAX_MEMBER_PARTITIONS.max(MAX_TOPICS) + 1
    );
    assert_eq!(
        subscription_limits.max_frame_bytes,
        MAX_SUBSCRIPTION_PAYLOAD_BYTES
    );
    assert_eq!(subscription_limits.max_string_bytes, MAX_KAFKA_STRING_BYTES);
    assert!(subscription_limits.max_frame_bytes > limits.max_frame_bytes);
}
