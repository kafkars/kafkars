//! Exact value-preservation scenarios for position-resolution failure facts.

use core::num::NonZeroI16;

use super::{PositionResolutionAttemptFailure, PositionResolutionFailure};

#[test]
fn signed_broker_code_remains_exact_inside_terminal_failure() {
    let Some(code) = NonZeroI16::new(-42) else {
        panic!("negative broker code is nonzero");
    };

    let failure =
        PositionResolutionFailure::Attempt(PositionResolutionAttemptFailure::Broker(code));
    let PositionResolutionFailure::Attempt(PositionResolutionAttemptFailure::Broker(actual)) =
        failure
    else {
        panic!("broker failure must retain its exact category");
    };
    assert_eq!(actual.get(), -42);
}
