//! Scenarios for simulator rejection of unsupported producer effects.

use kafka_client_core::{AdmissionSequence, FlushId, ProducerEffect};

use crate::{SimulationError, state::VirtualProducerState};

#[test]
fn flush_effects_are_rejected_until_completion_ownership_is_modeled() {
    for effect in [
        ProducerEffect::AcceptFlush {
            flush_id: FlushId::from_raw(1),
            barrier: AdmissionSequence::from_raw(1),
        },
        ProducerEffect::CompleteFlush {
            flush_id: FlushId::from_raw(1),
        },
    ] {
        let mut state = VirtualProducerState::default();

        assert_eq!(
            state.interpret(effect),
            Err(SimulationError::FlushControlUnavailable)
        );
        assert!(state.trace().is_empty());
    }
}
