//! Bounded plain-call ownership scenarios for transient Metadata calls.

use kafka_client_core::DescribeTopicsPlan;

use super::{
    describe_topics_calls::{DescribeTopicsCalls, describe_topics_minimum_version},
    describe_topics_submission::{
        DESCRIBE_TOPICS_AUTHORIZED_OPERATIONS_MIN_VERSION, DESCRIBE_TOPICS_ID_MIN_VERSION,
        DESCRIBE_TOPICS_MIN_VERSION,
    },
};

#[test]
fn call_capacity_is_explicit_and_non_growing() {
    let mut calls = DescribeTopicsCalls::new(1);
    assert!(calls.try_reserve().is_some());
    assert_eq!(calls.retained_count(), 0);
    assert!(calls.try_reserve().is_some());
}

#[test]
fn authorization_intent_raises_only_the_name_based_version_floor() {
    let default = DescribeTopicsPlan::new(vec!["orders".to_owned()])
        .unwrap_or_else(|error| panic!("default plan: {error}"));
    let authorized = default.clone().with_authorized_operations(true);
    let by_id = DescribeTopicsPlan::by_ids(vec![[1; 16]])
        .unwrap_or_else(|error| panic!("topic-ID plan: {error}"))
        .with_authorized_operations(true);
    assert_eq!(
        describe_topics_minimum_version(&default),
        DESCRIBE_TOPICS_MIN_VERSION
    );
    assert_eq!(
        describe_topics_minimum_version(&authorized),
        DESCRIBE_TOPICS_AUTHORIZED_OPERATIONS_MIN_VERSION
    );
    assert_eq!(
        describe_topics_minimum_version(&by_id),
        DESCRIBE_TOPICS_ID_MIN_VERSION
    );
}
