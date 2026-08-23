//! Lossless public share-acknowledgement admission evidence.

use std::time::Duration;

use super::ShareAcknowledgementAdmissionErrorKind;
use crate::consumer::share::registry_delivery_test::{finish, staged_handle};

#[test]
fn foreign_registry_rejection_returns_the_exact_capability() {
    let (first_owner, mut first_handle, first_group_id) = staged_handle();
    let (second_owner, second_handle, second_group_id) = staged_handle();
    let acknowledgement = first_handle
        .try_take_batch()
        .unwrap_or_else(|error| panic!("take: {error}"))
        .unwrap_or_else(|| panic!("batch"))
        .accept_all()
        .unwrap_or_else(|error| panic!("acknowledgement: {error}"));

    let error = second_handle
        .try_acknowledge(acknowledgement, Duration::from_secs(30))
        .err()
        .unwrap_or_else(|| panic!("foreign acknowledgement must reject"));
    assert_eq!(
        error.kind(),
        ShareAcknowledgementAdmissionErrorKind::ForeignRegistry
    );
    let acknowledgement = error.into_acknowledgement();
    assert_eq!(acknowledgement.acquisition_count(), 1);
    drop(acknowledgement);

    finish(first_owner, first_group_id);
    finish(second_owner, second_group_id);
}
