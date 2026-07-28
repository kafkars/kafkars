//! Shared scalar and version checks for dynamic and opt-in static Range membership.

use kafka_wire_core::DecodeLimits;

pub(crate) const JOIN_MIN_VERSION: i16 = 1;
pub(crate) const JOIN_MAX_VERSION: i16 = 3;
pub(crate) const STATIC_JOIN_VERSION: i16 = 5;
pub(crate) const SYNC_MIN_VERSION: i16 = 0;
pub(crate) const SYNC_MAX_VERSION: i16 = 2;
pub(crate) const STATIC_SYNC_VERSION: i16 = 3;
pub(crate) const HEARTBEAT_MIN_VERSION: i16 = 0;
pub(crate) const HEARTBEAT_MAX_VERSION: i16 = 2;
pub(crate) const STATIC_HEARTBEAT_VERSION: i16 = 3;
pub(crate) const LEAVE_MIN_VERSION: i16 = 0;
pub(crate) const LEAVE_MAX_VERSION: i16 = 2;
pub(crate) const STATIC_LEAVE_VERSION: i16 = 3;
pub(super) const INNER_SCHEMA_VERSION: i16 = 0;
pub(super) const PROTOCOL_TYPE: &str = "consumer";
pub(super) const RANGE_PROTOCOL: &str = "range";
pub(super) const MAX_MEMBERS: usize = 64;
pub(super) const MAX_TOPICS: usize = 64;
pub(crate) const MAX_MEMBER_PARTITIONS: usize = 64;
pub(super) const MAX_TOPIC_BYTES: usize = 249;
pub(super) const MAX_KAFKA_STRING_BYTES: usize = i16::MAX as usize;
pub(super) const MAX_MEMBER_NAME_BYTES: usize = MAX_MEMBERS * MAX_KAFKA_STRING_BYTES;
pub(super) const MAX_JOIN_TOPIC_NAME_BYTES: usize = MAX_MEMBERS * MAX_TOPICS * MAX_TOPIC_BYTES;
pub(super) const MAX_INNER_PAYLOAD_BYTES: usize =
    2 + 4 + MAX_TOPICS * (2 + MAX_TOPIC_BYTES + 4) + MAX_MEMBER_PARTITIONS * 4 + 4;

pub(super) fn inner_decode_limits() -> DecodeLimits {
    let mut limits = DecodeLimits::default();
    limits.max_frame_bytes = MAX_INNER_PAYLOAD_BYTES;
    limits.max_string_bytes = MAX_TOPIC_BYTES;
    limits.max_bytes_bytes = MAX_INNER_PAYLOAD_BYTES;
    limits.max_array_elements = MAX_TOPICS;
    limits.max_tagged_fields = 0;
    limits.max_tag_bytes = 0;
    limits.max_total_tag_bytes = 0;
    limits
}

pub(super) fn valid_join_version(version: i16) -> bool {
    (JOIN_MIN_VERSION..=JOIN_MAX_VERSION).contains(&version) || version == STATIC_JOIN_VERSION
}

pub(super) fn valid_sync_version(version: i16) -> bool {
    (SYNC_MIN_VERSION..=SYNC_MAX_VERSION).contains(&version) || version == STATIC_SYNC_VERSION
}

pub(super) const fn valid_topic(topic: &str) -> bool {
    !topic.is_empty() && topic.len() <= MAX_TOPIC_BYTES
}

pub(super) const fn valid_kafka_string(value: &str) -> bool {
    !value.is_empty() && value.len() <= MAX_KAFKA_STRING_BYTES
}
