//! Stable engine result and exhaustive core translation scenarios.

use core::num::NonZeroI16;

use kafka_client_core::{
    DescribeMetadataQuorumBrokerError as CoreBrokerError,
    DescribeMetadataQuorumListener as CoreListener, DescribeMetadataQuorumNode as CoreNode,
    DescribeMetadataQuorumReplica as CoreReplica, DescribeMetadataQuorumTerminal as CoreTerminal,
};

use super::{DescribeMetadataQuorumOutcome, outcome::translate_terminal};

#[test]
fn successful_terminal_remains_generated_free_and_lossless() {
    let replica = CoreReplica::new(1, Some([7; 16]), Some(42), Some(43), Some(44));
    let listener = CoreListener::new(
        "CONTROLLER".to_owned(),
        "controller.example".to_owned(),
        9093,
    );
    let description = kafka_client_core::DescribeMetadataQuorumDescription::new(
        Some(1),
        9,
        41,
        vec![replica],
        Vec::new(),
        Some(vec![CoreNode::new(1, vec![listener])]),
    )
    .unwrap_or_else(|error| panic!("description: {error}"));

    let DescribeMetadataQuorumOutcome::Described(description) =
        translate_terminal(CoreTerminal::Described(description))
    else {
        panic!("description expected");
    };
    assert_eq!(description.leader_id(), Some(1));
    assert_eq!(description.leader_epoch(), 9);
    assert_eq!(description.high_watermark(), 41);
    assert_eq!(
        description.voters()[0].replica_directory_id(),
        Some([7; 16])
    );
    assert_eq!(
        description.nodes().unwrap_or_default()[0].listeners()[0].port(),
        9093
    );
}

#[test]
fn exact_signed_top_level_error_and_diagnostic_remain_lossless() {
    let error = CoreBrokerError::new(
        NonZeroI16::new(-71).unwrap_or_else(|| panic!("nonzero")),
        Some("controller unavailable".to_owned()),
        true,
    );
    let DescribeMetadataQuorumOutcome::BrokerRejected(error) =
        translate_terminal(CoreTerminal::BrokerRejected(error))
    else {
        panic!("top-level error expected");
    };
    assert_eq!(error.code(), -71);
    assert_eq!(error.message(), Some("controller unavailable"));
    assert!(error.message_truncated());
}
