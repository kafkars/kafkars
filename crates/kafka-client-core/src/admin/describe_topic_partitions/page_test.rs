//! Aggregate duplicate, count, text, broker-reference, and cursor page tests.

#![expect(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::expect_used,
    reason = "bounded fixture indices and contextual failures are explicit test inputs"
)]

use super::{
    DESCRIBE_TOPIC_PARTITIONS_MAX_BROKER_REFERENCES,
    DESCRIBE_TOPIC_PARTITIONS_MAX_RESPONSE_PARTITIONS, DescribeTopicPartition,
    DescribeTopicPartitionsCursor, DescribeTopicPartitionsPage, DescribeTopicPartitionsTopic,
    DescribeTopicPartitionsValueError, value_test::partition,
};

#[test]
fn explicit_page_preserves_throttle_subset_and_next_cursor() {
    let next = DescribeTopicPartitionsCursor::new("orders".to_owned(), 9).expect("cursor");
    let page = DescribeTopicPartitionsPage::new(
        17,
        vec![topic("orders", vec![partition(7), partition(2)])],
        Some(next),
    )
    .expect("valid page");
    assert_eq!(page.throttle_time_ms(), 17);
    assert_eq!(page.topics()[0].name(), "orders");
    assert_eq!(page.topics()[0].partitions()[0].partition_index(), 7);
    assert_eq!(page.topics()[0].partitions()[1].partition_index(), 2);
    assert_eq!(
        page.next_cursor()
            .map(super::model::DescribeTopicPartitionsCursor::partition_index),
        Some(9)
    );
}

#[test]
fn duplicate_topics_and_hostile_partition_counts_are_rejected() {
    assert_eq!(
        DescribeTopicPartitionsPage::new(
            0,
            vec![topic("orders", Vec::new()), topic("orders", Vec::new())],
            None,
        ),
        Err(DescribeTopicPartitionsValueError::DuplicateTopic)
    );
    let partitions = (0..=DESCRIBE_TOPIC_PARTITIONS_MAX_RESPONSE_PARTITIONS)
        .map(|index| partition(index as i32))
        .collect();
    assert_eq!(
        DescribeTopicPartitionsPage::new(0, vec![topic("orders", partitions)], None,),
        Err(DescribeTopicPartitionsValueError::TooManyPartitions)
    );
}

#[test]
fn hostile_broker_reference_count_is_rejected() {
    let brokers = (0..=DESCRIBE_TOPIC_PARTITIONS_MAX_BROKER_REFERENCES)
        .map(|broker| broker as i32)
        .collect();
    let partition = DescribeTopicPartition::new(
        0,
        0,
        None,
        None,
        brokers,
        Vec::new(),
        None,
        None,
        Vec::new(),
    )
    .expect("locally valid unique broker list");
    assert_eq!(
        DescribeTopicPartitionsPage::new(0, vec![topic("orders", vec![partition])], None,),
        Err(DescribeTopicPartitionsValueError::TooManyBrokerReferences)
    );
}

#[test]
fn aggregate_text_and_retained_byte_envelopes_are_enforced() {
    let topics = (0..33)
        .map(|index| {
            let prefix = format!("{index:04}-");
            let name = format!("{prefix}{}", "x".repeat(i16::MAX as usize - prefix.len()));
            topic(&name, Vec::new())
        })
        .collect();
    assert_eq!(
        DescribeTopicPartitionsPage::new(0, topics, None),
        Err(DescribeTopicPartitionsValueError::TopicBytesExceeded)
    );

    let partitions = (0..DESCRIBE_TOPIC_PARTITIONS_MAX_RESPONSE_PARTITIONS)
        .map(|index| partition(index as i32))
        .collect();
    assert_eq!(
        DescribeTopicPartitionsPage::new(0, vec![topic("orders", partitions)], None,),
        Err(DescribeTopicPartitionsValueError::RetainedBytesExceeded)
    );
}

pub(super) fn topic(
    name: &str,
    partitions: Vec<DescribeTopicPartition>,
) -> DescribeTopicPartitionsTopic {
    DescribeTopicPartitionsTopic::new(0, name.to_owned(), [7; 16], false, partitions, i32::MIN)
        .expect("valid topic")
}
