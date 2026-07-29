//! Stable generated-free Admin `ProducerState` scenarios.

use super::ProducerState;

#[test]
fn state_preserves_exact_scalars_sentinels_and_optional_transaction_offset() {
    let state = ProducerState::new(71, 4, -1, -1, 9, None);

    assert_eq!(state.producer_id(), 71);
    assert_eq!(state.producer_epoch(), 4);
    assert_eq!(state.last_sequence(), -1);
    assert_eq!(state.last_timestamp(), -1);
    assert_eq!(state.coordinator_epoch(), 9);
    assert_eq!(state.current_transaction_start_offset(), None);

    let active = ProducerState::new(72, 5, 12, 1_700_000_000_123, 10, Some(91));
    assert_eq!(active.current_transaction_start_offset(), Some(91));
}
