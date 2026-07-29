//! Scenarios for stable active-producer scalar facts.

use super::AdminProducerState;

#[test]
fn producer_state_preserves_all_api_61_v0_facts() {
    let state = AdminProducerState::new(91, 7, 42, 1_700_000_000_123, 5, Some(88));

    assert_eq!(state.producer_id(), 91);
    assert_eq!(state.producer_epoch(), 7);
    assert_eq!(state.last_sequence(), 42);
    assert_eq!(state.last_timestamp(), 1_700_000_000_123);
    assert_eq!(state.coordinator_epoch(), 5);
    assert_eq!(state.current_transaction_start_offset(), Some(88));
    assert_eq!(
        state.into_parts(),
        (91, 7, 42, 1_700_000_000_123, 5, Some(88))
    );
}

#[test]
fn initial_sentinels_are_stable_but_lower_or_negative_identity_values_are_invalid() {
    assert!(AdminProducerState::new(1, 0, -1, -1, 0, None).is_well_formed());
    assert!(!AdminProducerState::new(-1, 0, -1, -1, 0, None).is_well_formed());
    assert!(!AdminProducerState::new(1, -1, -1, -1, 0, None).is_well_formed());
    assert!(!AdminProducerState::new(1, 0, -2, -1, 0, None).is_well_formed());
    assert!(!AdminProducerState::new(1, 0, -1, -2, 0, None).is_well_formed());
    assert!(!AdminProducerState::new(1, 0, -1, -1, -1, None).is_well_formed());
}
