//! Exact all-topic reservation-limit scenarios.

use super::limits::{DESCRIBE_TOPICS_RETAINED_BYTES, all_topics_retained_charge};

#[test]
fn all_topic_charge_is_the_existing_complete_host_envelope() {
    assert_eq!(all_topics_retained_charge(), DESCRIBE_TOPICS_RETAINED_BYTES);
}
