//! Producer identity and control-batch coherence after wire decoding.

use super::{
    batch_model::normalize_batch,
    batch_model_test::{batch, test_budget},
    failure::FetchDecodeFailure,
};

#[test]
fn producer_identity_is_coherent_and_required_for_transactions() {
    let mut budget = test_budget();
    let mut identified = batch();
    identified.producer_id = 7;
    identified.producer_epoch = 2;
    identified.base_sequence = 4;
    identified.is_transactional = true;
    let normalized = normalize_batch(identified, &mut budget)
        .unwrap_or_else(|error| panic!("coherent producer tuple: {error:?}"));
    let identity = normalized
        .producer
        .unwrap_or_else(|| panic!("transactional batch identity"));
    assert_eq!(identity.producer_id, 7);
    assert_eq!(identity.producer_epoch, 2);
    assert_eq!(identity.base_sequence, 4);

    let mut budget = test_budget();
    let mut missing = batch();
    missing.is_transactional = true;
    assert_eq!(
        normalize_batch(missing, &mut budget),
        Err(FetchDecodeFailure::TransactionalIdentityMissing)
    );

    let mut budget = test_budget();
    let mut mixed = batch();
    mixed.producer_id = 7;
    assert_eq!(
        normalize_batch(mixed, &mut budget),
        Err(FetchDecodeFailure::InvalidProducerIdentity {
            producer_id: 7,
            producer_epoch: -1,
            base_sequence: -1,
        })
    );
}

#[test]
fn control_batch_identity_matches_transactional_mode_and_exact_sequence_sentinel() {
    let mut budget = test_budget();
    let mut control = batch();
    control.producer_id = 7;
    control.producer_epoch = 2;
    control.base_sequence = -1;
    control.is_transactional = true;
    control.is_control = true;
    let normalized = normalize_batch(control, &mut budget)
        .unwrap_or_else(|error| panic!("coherent control identity: {error:?}"));
    assert_eq!(
        normalized.producer.map(|identity| identity.base_sequence),
        Some(-1)
    );

    for (control, transactional, base_sequence, expected) in [
        (
            true,
            true,
            0,
            FetchDecodeFailure::InvalidProducerIdentity {
                producer_id: 7,
                producer_epoch: 2,
                base_sequence: 0,
            },
        ),
        (
            false,
            true,
            -1,
            FetchDecodeFailure::InvalidProducerIdentity {
                producer_id: 7,
                producer_epoch: 2,
                base_sequence: -1,
            },
        ),
        (
            true,
            false,
            -1,
            FetchDecodeFailure::NonTransactionalControlIdentity,
        ),
    ] {
        let mut budget = test_budget();
        let mut malformed = batch();
        malformed.producer_id = 7;
        malformed.producer_epoch = 2;
        malformed.base_sequence = base_sequence;
        malformed.is_transactional = transactional;
        malformed.is_control = control;
        assert_eq!(normalize_batch(malformed, &mut budget), Err(expected));
    }
}
