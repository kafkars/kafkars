//! Canonical bounded-storage scenarios for public `CreateTopics` request values.

use super::{CreateTopic, CreateTopicConfig, CreateTopicsRequest};

#[test]
fn excess_input_capacity_is_removed_before_retained_byte_charging() {
    let mut name = String::with_capacity(1024 * 1024);
    name.push_str("orders");
    let mut value = String::with_capacity(512 * 1024);
    value.push_str("compact");
    let mut topics = Vec::with_capacity(4096);
    topics.push(
        CreateTopic::new(name, 3)
            .with_config(CreateTopicConfig::new("cleanup.policy", Some(value))),
    );
    let request = CreateTopicsRequest::new(topics);
    assert!(!request.storage_is_canonical());

    let canonical = request.canonicalize();
    assert!(canonical.storage_is_canonical());
    assert!(
        canonical
            .retained_charge()
            .is_some_and(|charge| charge < 32 * 1024)
    );
}
