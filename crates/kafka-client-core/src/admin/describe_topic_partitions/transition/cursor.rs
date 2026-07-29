//! Request-cursor and next-cursor ordering checks for one explicit page.

use super::super::{
    DescribeTopicPartitionsCursor, DescribeTopicPartitionsPage, DescribeTopicPartitionsPlan,
};

pub(super) fn request_cursor_allows_page(
    plan: &DescribeTopicPartitionsPlan,
    page: &DescribeTopicPartitionsPage,
) -> bool {
    let Some(cursor) = plan.cursor() else {
        return true;
    };
    let cursor_topic = plan
        .topics()
        .iter()
        .position(|topic| topic.as_bytes() == cursor.topic_name().as_bytes());
    page.topics().iter().all(|topic| {
        let response_topic = plan
            .topics()
            .iter()
            .position(|requested| requested.as_bytes() == topic.name().as_bytes());
        match (cursor_topic, response_topic) {
            (Some(cursor_rank), Some(response_rank)) if response_rank > cursor_rank => true,
            (Some(cursor_rank), Some(response_rank)) if response_rank == cursor_rank => topic
                .partitions()
                .iter()
                .all(|partition| partition.partition_index() >= cursor.partition_index()),
            _ => false,
        }
    })
}

pub(super) fn next_cursor_advances(
    plan: &DescribeTopicPartitionsPlan,
    page: &DescribeTopicPartitionsPage,
) -> bool {
    let Some(next) = page.next_cursor() else {
        return true;
    };
    let Some(next_rank) = cursor_rank(plan, next) else {
        return false;
    };
    if plan
        .cursor()
        .and_then(|cursor| cursor_rank(plan, cursor))
        .is_some_and(|requested| next_rank <= requested)
    {
        return false;
    }
    page.topics().iter().all(|topic| {
        let Some(topic_rank) = plan
            .topics()
            .iter()
            .position(|requested| requested.as_bytes() == topic.name().as_bytes())
        else {
            return false;
        };
        if topic.partitions().is_empty() {
            return next_rank.0 >= topic_rank;
        }
        topic
            .partitions()
            .iter()
            .all(|partition| next_rank > (topic_rank, partition.partition_index()))
    })
}

fn cursor_rank(
    plan: &DescribeTopicPartitionsPlan,
    cursor: &DescribeTopicPartitionsCursor,
) -> Option<(usize, i32)> {
    plan.topics()
        .iter()
        .position(|topic| topic.as_bytes() == cursor.topic_name().as_bytes())
        .map(|rank| (rank, cursor.partition_index()))
}
