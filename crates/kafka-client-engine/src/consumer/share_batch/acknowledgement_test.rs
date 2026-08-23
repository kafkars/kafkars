//! Exact batch conversion, decision correlation, and drop-abandonment evidence.

use kafka_client_core::{
    ShareAcknowledgeType, ShareAcknowledgementBuildErrorKind, ShareDisposition,
};

use super::ShareConsumerBatch;
use crate::consumer::share::registry_delivery_test::{finish, staged_handle};

#[test]
fn accept_all_consumes_the_batch_and_drop_returns_exact_acquisitions() {
    let (owner, mut handle, group_id) = staged_handle();
    let batch = take_batch(&mut handle);

    let acknowledgement = batch
        .accept_all()
        .unwrap_or_else(|error| panic!("accept all: {error}"));

    assert_eq!(acknowledgement.acquisition_count(), 1);
    assert_eq!(acknowledgement.range_count(), 1);
    assert_eq!(
        acknowledgement
            .inner
            .as_ref()
            .unwrap_or_else(|| panic!("core acknowledgement"))
            .batches()[0]
            .acknowledge_types(),
        &[ShareAcknowledgeType::Accept]
    );
    drop(acknowledgement);
    finish(owner, group_id);
}

#[test]
fn record_decision_preserves_release_without_exposing_acquisition_identity() {
    let (owner, mut handle, group_id) = staged_handle();
    let batch = take_batch(&mut handle);
    let decisions = batch
        .records()
        .map(|record| record.decision(ShareDisposition::Release))
        .collect();

    let acknowledgement = batch
        .into_acknowledgement(decisions)
        .unwrap_or_else(|error| panic!("mixed acknowledgement: {error}"));

    assert_eq!(
        acknowledgement
            .inner
            .as_ref()
            .unwrap_or_else(|| panic!("core acknowledgement"))
            .batches()[0]
            .acknowledge_types(),
        &[ShareAcknowledgeType::Release]
    );
    drop(acknowledgement);
    finish(owner, group_id);
}

#[test]
fn normalization_rejection_returns_the_exact_batch_and_decisions() {
    let (owner, mut handle, group_id) = staged_handle();
    let batch = take_batch(&mut handle);
    let error = batch
        .into_acknowledgement(Vec::new())
        .err()
        .unwrap_or_else(|| panic!("missing decisions must reject"));

    assert_eq!(
        error.kind(),
        ShareAcknowledgementBuildErrorKind::EmptyDecisions
    );
    let (batch, decisions) = error.into_parts();
    assert!(decisions.is_empty());
    assert_eq!(batch.record_count(), 1);
    drop(batch);
    finish(owner, group_id);
}

fn take_batch(handle: &mut crate::consumer::share::ShareConsumerHandle) -> ShareConsumerBatch {
    handle
        .try_take_batch()
        .unwrap_or_else(|error| panic!("take batch: {error}"))
        .unwrap_or_else(|| panic!("staged batch"))
}
