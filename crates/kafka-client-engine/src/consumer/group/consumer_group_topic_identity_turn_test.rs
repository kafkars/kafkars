//! Topic lookup failures retain exact terminal membership categories.

use kafka_client_core::ConsumerGroupHeartbeatFailure;

use crate::driver::TopicPartitionCountFailure;

use super::consumer_group_topic_identity_turn::topic_lookup_failure;

#[test]
fn lookup_deadline_broker_and_malformed_facts_stay_distinct() {
    assert_eq!(
        topic_lookup_failure(TopicPartitionCountFailure::Deadline),
        ConsumerGroupHeartbeatFailure::DeadlineElapsed
    );
    assert_eq!(
        topic_lookup_failure(TopicPartitionCountFailure::Broker(17)),
        ConsumerGroupHeartbeatFailure::Broker(17)
    );
    assert_eq!(
        topic_lookup_failure(TopicPartitionCountFailure::Malformed),
        ConsumerGroupHeartbeatFailure::InvalidResponse
    );
    assert_eq!(
        topic_lookup_failure(TopicPartitionCountFailure::Unavailable),
        ConsumerGroupHeartbeatFailure::Execution
    );
}
