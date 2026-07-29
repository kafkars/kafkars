//! Validate-first producer-state normalization and deterministic ordering.

use kafka_wire::describe_producers_response::ProducerState;

use super::{
    DescribeProducersProtocolFailure, NormalizedProducerState,
    model::DESCRIBE_PRODUCERS_MAX_STATES,
    retention::{ensure_limit, states_charge},
};

pub(super) fn normalized_states(
    source: &[ProducerState],
    retained_limit: usize,
) -> Result<(Vec<NormalizedProducerState>, usize), DescribeProducersProtocolFailure> {
    if source.len() > DESCRIBE_PRODUCERS_MAX_STATES {
        return Err(DescribeProducersProtocolFailure::TooManyProducerStates {
            actual: source.len(),
            max: DESCRIBE_PRODUCERS_MAX_STATES,
        });
    }
    let minimum = states_charge(source.len()).unwrap_or(usize::MAX);
    ensure_limit(minimum, retained_limit)?;
    for state in source {
        validate_state(state)?;
    }

    let mut states = Vec::new();
    states.try_reserve_exact(source.len()).map_err(|_| {
        DescribeProducersProtocolFailure::Allocation {
            field: "active_producers",
            requested: source.len(),
        }
    })?;
    states.extend(source.iter().map(normalized_state));
    let retained = states_charge(states.capacity()).unwrap_or(usize::MAX);
    ensure_limit(retained, retained_limit)?;
    states.sort_unstable_by_key(NormalizedProducerState::producer_id);
    if let Some(pair) = states
        .windows(2)
        .find(|pair| pair[0].producer_id() == pair[1].producer_id())
    {
        return Err(DescribeProducersProtocolFailure::DuplicateProducerId {
            actual: pair[0].producer_id(),
        });
    }
    Ok((states, retained))
}

fn validate_state(state: &ProducerState) -> Result<(), DescribeProducersProtocolFailure> {
    if state.producer_id < 0 {
        return Err(DescribeProducersProtocolFailure::NegativeProducerId {
            actual: state.producer_id,
        });
    }
    if state.producer_epoch < 0 {
        return Err(DescribeProducersProtocolFailure::NegativeProducerEpoch {
            actual: state.producer_epoch,
        });
    }
    if state.last_sequence < -1 {
        return Err(DescribeProducersProtocolFailure::InvalidLastSequence {
            actual: state.last_sequence,
        });
    }
    if state.last_timestamp < -1 {
        return Err(DescribeProducersProtocolFailure::InvalidLastTimestamp {
            actual: state.last_timestamp,
        });
    }
    if state.coordinator_epoch < 0 {
        return Err(DescribeProducersProtocolFailure::NegativeCoordinatorEpoch {
            actual: state.coordinator_epoch,
        });
    }
    if state.current_txn_start_offset < -1 {
        return Err(
            DescribeProducersProtocolFailure::InvalidCurrentTransactionStartOffset {
                actual: state.current_txn_start_offset,
            },
        );
    }
    Ok(())
}

const fn normalized_state(state: &ProducerState) -> NormalizedProducerState {
    NormalizedProducerState::new(
        state.producer_id,
        state.producer_epoch,
        state.last_sequence,
        state.last_timestamp,
        state.coordinator_epoch,
        if state.current_txn_start_offset == -1 {
            None
        } else {
            Some(state.current_txn_start_offset)
        },
    )
}
