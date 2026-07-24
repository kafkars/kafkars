//! Stable cluster-description ownership scenarios.

use super::{ClusterBroker, ClusterDescription};

#[test]
fn nullable_rack_and_deterministic_broker_order_are_preserved() {
    let description = ClusterDescription::new(
        String::from("cluster-a"),
        Some(7),
        vec![
            ClusterBroker::new(2, String::from("b"), 9093, None, false),
            ClusterBroker::new(
                7,
                String::from("c"),
                9094,
                Some(String::from("rack-a")),
                true,
            ),
        ],
    );
    assert_eq!(description.cluster_id(), "cluster-a");
    assert_eq!(description.controller_id(), Some(7));
    assert_eq!(description.brokers()[0].id(), 2);
    assert_eq!(description.brokers()[0].host(), "b");
    assert_eq!(description.brokers()[0].port(), 9093);
    assert_eq!(description.brokers()[0].rack(), None);
    assert_eq!(description.brokers()[1].rack(), Some("rack-a"));
    assert!(!description.brokers()[0].is_fenced());
    assert!(description.brokers()[1].is_fenced());
    assert_eq!(description.authorized_operations(), None);
}
