//! Stable engine topic-terminal vocabulary scenarios.

use super::{DescribeTopicsDeliveryStatus, DescribeTopicsObserverError};

#[test]
fn delivery_certainty_values_remain_distinct() {
    assert_ne!(
        DescribeTopicsDeliveryStatus::NotSent,
        DescribeTopicsDeliveryStatus::PossiblySent
    );
}

#[test]
fn observer_failures_have_stable_diagnostics() {
    assert_eq!(
        DescribeTopicsObserverError::AlreadyObserved.to_string(),
        "DescribeTopics result was already observed"
    );
    assert_eq!(
        DescribeTopicsObserverError::Stale.to_string(),
        "DescribeTopics observer is stale"
    );
}
