//! Scenarios for simulator rejection of unowned producer effects.

use kafka_client_core::{AdmissionSequence, FlushId, ProducerEffect};

use crate::{SimulationError, state::VirtualProducerState};

#[test]
fn flush_effects_require_owned_completion_state() {
    let mut state = VirtualProducerState::default();
    assert_eq!(
        state.interpret(ProducerEffect::AcceptFlush {
            flush_id: FlushId::from_raw(1),
            barrier: AdmissionSequence::from_raw(1),
        }),
        Err(SimulationError::MissingFlushReservation(FlushId::from_raw(
            1
        )))
    );
    assert_eq!(
        state.interpret(ProducerEffect::CompleteFlush {
            flush_id: FlushId::from_raw(1),
        }),
        Err(SimulationError::UnknownFlush(FlushId::from_raw(1)))
    );
    assert!(state.trace().is_empty());
}
