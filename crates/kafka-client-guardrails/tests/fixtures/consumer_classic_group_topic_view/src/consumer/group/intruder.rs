//! Deliberate driver topology-view escape into group policy composition.

use kafka_driver::TopicView;

fn invent_topology_policy(view: &TopicView) -> u32 {
    view.logical_partition_count()
}
