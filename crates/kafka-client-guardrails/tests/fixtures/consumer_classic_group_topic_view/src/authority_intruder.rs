//! Deliberate foreign construction of the accepted topic-view call owner.

use crate::driver::rpc::topic_view::TopicPartitionCountCall;

fn forge() -> TopicPartitionCountCall {
    TopicPartitionCountCall {
        topic_view_topic: 1,
        topic_view_driver_call: 2,
    }
}
