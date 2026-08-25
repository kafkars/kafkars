//! Public transaction-validation ownership and signature contract.

use std::{
    future::Future,
    time::{Duration, Instant},
};

use crate::{ErrorKind, KafkaError};

use super::transaction::validation_deadline_at;
use super::{Transaction, ValidateTransaction};

#[test]
fn validation_exclusively_reborrows_the_active_transaction() {
    fn require_validation<'validation, 'producer>(
        _method: fn(
            &'validation mut Transaction<'producer>,
            Duration,
        ) -> Result<ValidateTransaction<'validation, 'producer>, KafkaError>,
    ) {
    }
    fn require_future<T: Future<Output = Result<(), KafkaError>>>() {}
    fn require_wait(_method: fn(ValidateTransaction<'static, 'static>) -> Result<(), KafkaError>) {}

    require_validation(Transaction::validate_for_commit);
    require_future::<ValidateTransaction<'static, 'static>>();
    require_wait(ValidateTransaction::wait);
}

#[test]
fn validation_deadline_starts_at_the_public_validation_boundary() {
    let boundary = Instant::now();
    let timeout = Duration::from_nanos(41);
    let expected = boundary
        .checked_add(timeout)
        .unwrap_or_else(|| panic!("small validation deadline should be representable"));

    assert_eq!(
        validation_deadline_at(boundary, timeout)
            .unwrap_or_else(|error| panic!("capture validation deadline: {error}")),
        expected
    );
    let error = validation_deadline_at(boundary, Duration::ZERO)
        .err()
        .unwrap_or_else(|| panic!("zero validation timeout must reject"));
    assert_eq!(error.kind(), ErrorKind::Timeout);
}
