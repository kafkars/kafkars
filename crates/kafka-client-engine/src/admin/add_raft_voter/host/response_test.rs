//! Exhaustive protocol-failure and top-level status classification.

use kafka_client_core::{AddRaftVoterInput, DeliveryStatus};

use crate::protocol::admin::add_raft_voter::AddRaftVoterResponseFailure;

use super::response::{normalized_input, protocol_failure};

#[test]
fn compatibility_capacity_and_malformed_shapes_remain_distinct() {
    assert_eq!(
        protocol_failure(AddRaftVoterResponseFailure::MissingSelectedVersion),
        AddRaftVoterInput::ProtocolIncompatible {
            delivery: DeliveryStatus::PossiblySent,
        }
    );
    assert_eq!(
        protocol_failure(AddRaftVoterResponseFailure::RetainedBytes {
            required: 11,
            limit: 10,
        }),
        AddRaftVoterInput::ResponseTooLarge
    );
    assert_eq!(
        protocol_failure(AddRaftVoterResponseFailure::NegativeThrottleTime { actual: -1 }),
        AddRaftVoterInput::InvalidResponse
    );
}

#[test]
fn success_diagnostic_is_invalid_but_signed_error_preserves_diagnostic() {
    assert_eq!(
        normalized_input(0, 0, Some("unexpected".to_owned()), false),
        AddRaftVoterInput::InvalidResponse
    );
    let AddRaftVoterInput::BrokerRejected { error } =
        normalized_input(7, -32_000, Some("denied".to_owned()), true)
    else {
        panic!("broker rejection expected");
    };
    assert_eq!(
        error.into_parts(),
        (7, -32_000, Some("denied".to_owned()), true)
    );
}
