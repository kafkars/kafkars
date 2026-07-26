//! Stable transactional producer identity scenarios.

use super::TransactionalProducerIdentity;

#[test]
fn identity_preserves_broker_scalars_exactly() {
    let identity = TransactionalProducerIdentity::new(41, 3);
    assert_eq!(identity.producer_id(), 41);
    assert_eq!(identity.producer_epoch(), 3);
}
