//! Lossless duplication of due timer inputs for apply and retained-fault ownership.

use kafka_client_core::AssignedConsumerInput;

pub(super) fn duplicate_due_input(
    input: AssignedConsumerInput,
) -> Result<(AssignedConsumerInput, AssignedConsumerInput), AssignedConsumerInput> {
    match input {
        AssignedConsumerInput::PositionThrottleElapsed { fence, now } => Ok((
            AssignedConsumerInput::PositionThrottleElapsed { fence, now },
            AssignedConsumerInput::PositionThrottleElapsed { fence, now },
        )),
        AssignedConsumerInput::FetchThrottleElapsed { fence, now } => Ok((
            AssignedConsumerInput::FetchThrottleElapsed { fence, now },
            AssignedConsumerInput::FetchThrottleElapsed { fence, now },
        )),
        input => Err(input),
    }
}
