//! Public-to-engine broker and topic-partition selection translation tests.

use kafka_client_engine::{DescribeLogDirTarget, DescribeLogDirsRequest};

use crate::{StartPosition, TopicPartition};

use super::request::DescribeLogDirsAdminRequest;

#[test]
fn default_translation_preserves_all_topics_and_broker_order() {
    let request = DescribeLogDirsAdminRequest::new(vec![7, 2]);

    assert_eq!(
        request.into_engine(),
        DescribeLogDirsRequest::all(vec![7, 2])
    );
}

#[test]
fn selected_translation_preserves_partition_order_and_replaces_selection() {
    let request = DescribeLogDirsAdminRequest::new(vec![7, 2])
        .with_partitions(vec![
            TopicPartition::new("discarded", 9),
            TopicPartition::new("discarded", 10),
        ])
        .with_partitions(vec![
            TopicPartition::new("zeta", 3),
            TopicPartition::new("audit", 1),
        ]);

    assert_eq!(
        request.into_engine(),
        DescribeLogDirsRequest::selected(
            vec![7, 2],
            vec![
                DescribeLogDirTarget::new("zeta".to_owned(), 3),
                DescribeLogDirTarget::new("audit".to_owned(), 1),
            ],
        )
    );
}

#[test]
fn empty_explicit_selection_is_retained_for_submit_time_rejection() {
    let request =
        DescribeLogDirsAdminRequest::new(vec![7]).with_partitions(Vec::<TopicPartition>::new());

    assert_eq!(
        request.into_engine(),
        DescribeLogDirsRequest::selected(vec![7], Vec::new())
    );
}

#[test]
fn assignment_only_start_position_is_preserved_as_invalid_input() {
    let request = DescribeLogDirsAdminRequest::new(vec![7]).with_partitions(vec![
        TopicPartition::new("orders", 2).start_at(StartPosition::End),
    ]);

    assert_eq!(
        request.into_engine(),
        DescribeLogDirsRequest::selected(
            vec![7],
            vec![DescribeLogDirTarget::new("orders".to_owned(), i32::MIN)],
        )
    );
}
