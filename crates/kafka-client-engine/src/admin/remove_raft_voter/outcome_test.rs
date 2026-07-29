//! Generated-free successful and broker-rejected voter-removal translation.

use core::num::NonZeroI16;

use kafka_client_core::{
    RemoveRaftVoterBrokerError as CoreBrokerError, RemoveRaftVoterSuccess, RemoveRaftVoterTerminal,
};

use super::{RemoveRaftVoterOutcome, outcome::translate_terminal};

#[test]
fn successful_result_exposes_throttle_as_scalar_part() {
    let RemoveRaftVoterOutcome::Removed(result) = translate_terminal(
        RemoveRaftVoterTerminal::Removed(RemoveRaftVoterSuccess::new(17)),
    ) else {
        panic!("success expected");
    };
    assert_eq!(result.throttle_time_ms(), 17);
    assert_eq!(result.into_parts(), 17);
}

#[test]
fn broker_error_preserves_signed_code_and_bounded_message() {
    let core = CoreBrokerError::new(
        23,
        NonZeroI16::new(-7).unwrap_or_else(|| panic!("nonzero")),
        Some("rejected".to_owned()),
        true,
    );
    let RemoveRaftVoterOutcome::BrokerRejected(error) =
        translate_terminal(RemoveRaftVoterTerminal::BrokerRejected(core))
    else {
        panic!("broker rejection expected");
    };
    assert_eq!(
        error.into_parts(),
        (23, -7, Some("rejected".to_owned()), true)
    );
}
