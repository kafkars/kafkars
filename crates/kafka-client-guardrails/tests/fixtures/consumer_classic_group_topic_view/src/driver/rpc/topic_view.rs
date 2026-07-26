//! Sanctioned driver-owned immutable topic-view adapter.

use kafka_driver::TopicView;

struct TopicPartitionCountCall {
    topic_view_topic: usize,
    topic_view_driver_call: usize,
}

fn logical_partition_count(view: &TopicView) -> u32 {
    view.logical_partition_count()
}
