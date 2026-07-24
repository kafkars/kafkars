//! Stable `DescribeCluster` outcome translation scenarios.

use kafka_client_core::{
    ClusterBroker as CoreClusterBroker, ClusterDescription as CoreClusterDescription,
    DescribeClusterTerminal,
};

use super::{DescribeClusterOutcome, describe_outcome::translate_terminal};

#[test]
fn controller_identity_survives_core_to_engine_translation() {
    let core = CoreClusterDescription::new_with_authorized_operations(
        String::from("cluster-a"),
        Some(7),
        vec![CoreClusterBroker::new(
            7,
            String::from("broker.local"),
            9092,
            None,
            true,
        )],
        Some(0x1234),
    );
    let DescribeClusterOutcome::Cluster(engine) =
        translate_terminal(DescribeClusterTerminal::Cluster(core))
    else {
        panic!("cluster terminal must remain a cluster terminal");
    };
    assert_eq!(engine.cluster_id(), "cluster-a");
    assert_eq!(engine.controller_id(), Some(7));
    assert_eq!(engine.brokers()[0].id(), 7);
    assert!(engine.brokers()[0].is_fenced());
    assert_eq!(engine.authorized_operations(), Some(0x1234));
}
