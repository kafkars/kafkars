//! Modern topic-identity capacity, translation, and owned-partition evidence.

use kafka_client_core::{GroupAssignmentPartition, PartitionIndex, TopicId};

use crate::{
    driver::TopicPartitionCountFact, protocol::consumer::ConsumerGroupHeartbeatAssignmentTopic,
};

use super::consumer_group_topic_identity::{
    ConsumerGroupTopicIdentityError, ConsumerGroupTopicIdentityOwner,
};

#[test]
fn resolved_topic_uuids_translate_assignments_and_owned_partitions() {
    let first = TopicId::from_raw(1);
    let second = TopicId::from_raw(2);
    let mut owner = ConsumerGroupTopicIdentityOwner::try_new(2)
        .unwrap_or_else(|error| panic!("owner: {error:?}"));
    owner
        .append(first, fact([1; 16], 3))
        .unwrap_or_else(|error| panic!("first: {error:?}"));
    owner
        .append(second, fact([2; 16], 2))
        .unwrap_or_else(|error| panic!("second: {error:?}"));

    let translated = owner
        .translate_assignment(&[
            ConsumerGroupHeartbeatAssignmentTopic::new([2; 16], vec![1]),
            ConsumerGroupHeartbeatAssignmentTopic::new([1; 16], vec![2, 0]),
        ])
        .unwrap_or_else(|error| panic!("translate: {error:?}"));
    assert_eq!(
        translated,
        vec![
            GroupAssignmentPartition::new(first, PartitionIndex::from_raw(0)),
            GroupAssignmentPartition::new(first, PartitionIndex::from_raw(2)),
            GroupAssignmentPartition::new(second, PartitionIndex::from_raw(1)),
        ]
    );

    let owned = owner
        .owned_topics(&translated)
        .unwrap_or_else(|error| panic!("owned: {error:?}"));
    assert_eq!(owned.len(), 2);
}

#[test]
fn missing_duplicate_unknown_and_out_of_range_identities_are_rejected() {
    let topic = TopicId::from_raw(1);
    let mut owner = ConsumerGroupTopicIdentityOwner::try_new(1)
        .unwrap_or_else(|error| panic!("owner: {error:?}"));
    assert_eq!(
        owner.append(
            topic,
            TopicPartitionCountFact {
                metadata_generation: 1,
                logical_partition_count: 1,
                kafka_topic_id: None,
            }
        ),
        Err(ConsumerGroupTopicIdentityError::MissingKafkaTopicId)
    );
    owner
        .append(topic, fact([7; 16], 1))
        .unwrap_or_else(|error| panic!("append: {error:?}"));
    assert_eq!(
        owner
            .translate_assignment(&[ConsumerGroupHeartbeatAssignmentTopic::new([8; 16], vec![0],)]),
        Err(ConsumerGroupTopicIdentityError::UnknownKafkaTopic)
    );
    assert_eq!(
        owner
            .translate_assignment(&[ConsumerGroupHeartbeatAssignmentTopic::new([7; 16], vec![1],)]),
        Err(ConsumerGroupTopicIdentityError::PartitionOutOfRange)
    );
}

fn fact(kafka_topic_id: [u8; 16], logical_partition_count: u32) -> TopicPartitionCountFact {
    TopicPartitionCountFact {
        metadata_generation: 1,
        logical_partition_count,
        kafka_topic_id: Some(kafka_topic_id),
    }
}
