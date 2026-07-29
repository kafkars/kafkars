//! Stable engine page and exhaustive core translation scenarios.

use kafka_client_core::{
    DescribeTopicPartition as CorePartition, DescribeTopicPartitionsCursor as CoreCursor,
    DescribeTopicPartitionsPage as CorePage, DescribeTopicPartitionsTerminal as CoreTerminal,
    DescribeTopicPartitionsTopic as CoreTopic,
};

use super::{AdminDescribeTopicPartitionsOutcome, outcome::translate_terminal};

#[test]
fn page_translation_is_lossless_and_consumable_without_cloning() {
    let partition = CorePartition::new(
        -5,
        3,
        Some(1),
        Some(7),
        vec![1, 2],
        vec![1],
        Some(vec![2]),
        None,
        vec![2],
    )
    .unwrap_or_else(|error| panic!("partition: {error}"));
    let topic = CoreTopic::new(
        -9,
        "orders".to_owned(),
        [4; 16],
        true,
        vec![partition],
        i32::MIN,
    )
    .unwrap_or_else(|error| panic!("topic: {error}"));
    let cursor =
        CoreCursor::new("orders".to_owned(), 4).unwrap_or_else(|error| panic!("cursor: {error}"));
    let page = CorePage::new(11, vec![topic], Some(cursor))
        .unwrap_or_else(|error| panic!("page: {error}"));

    let AdminDescribeTopicPartitionsOutcome::Page(page) =
        translate_terminal(CoreTerminal::Page(page))
    else {
        panic!("page expected");
    };
    let (throttle, topics, cursor) = page.into_parts();
    assert_eq!(throttle, 11);
    assert_eq!(
        cursor.map(|value| value.into_parts()),
        Some(("orders".to_owned(), 4))
    );
    let (error, name, topic_id, internal, partitions, operations) = topics
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("topic"))
        .into_parts();
    assert_eq!(
        (error, name.as_str(), topic_id, internal, operations),
        (-9, "orders", [4; 16], true, i32::MIN)
    );
    let (error, index, leader, epoch, replicas, isr, eligible, last_known, offline) = partitions
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("partition"))
        .into_parts();
    assert_eq!((error, index, leader, epoch), (-5, 3, Some(1), Some(7)));
    assert_eq!(replicas, [1, 2]);
    assert_eq!(isr, [1]);
    assert_eq!(eligible, Some(vec![2]));
    assert_eq!(last_known, None);
    assert_eq!(offline, [2]);
}
