//! Deliberately cloneable immutable topic-view call ownership.

#[derive(Clone, Copy)]
struct TopicPartitionCountCall {
    topic_view_topic: usize,
    topic_view_driver_call: usize,
}
