//! Join submission readiness waits for every bounded topic identity.

use std::time::Duration;

use kafka_client_core::{GroupId, TopicId};

use crate::{clock::MonotonicClock, driver::TopicPartitionCountFact};

use super::{
    consumer_group_execution::ConsumerGroupExecution,
    consumer_group_heartbeat_submission::consumer_group_execution_is_ready,
};

#[test]
fn join_waits_until_all_topic_uuids_are_retained() {
    let mut execution = ConsumerGroupExecution::try_new(group_id(), 1, 30_000)
        .unwrap_or_else(|error| panic!("execution: {error:?}"));
    let capture = MonotonicClock::new()
        .capture_deadline_after(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("capture: {error:?}"));
    execution
        .begin(capture)
        .unwrap_or_else(|error| panic!("begin: {error:?}"));
    assert!(!consumer_group_execution_is_ready(&execution));
    execution
        .topic_identities_mut()
        .append(
            TopicId::from_raw(1),
            TopicPartitionCountFact {
                metadata_generation: 1,
                logical_partition_count: 3,
                kafka_topic_id: Some([4; 16]),
            },
        )
        .unwrap_or_else(|error| panic!("identity: {error:?}"));
    assert!(consumer_group_execution_is_ready(&execution));
}

fn group_id() -> GroupId {
    GroupId::try_from_raw(1).unwrap_or_else(|| panic!("group id"))
}
