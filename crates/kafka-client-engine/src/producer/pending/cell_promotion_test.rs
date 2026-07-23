//! Pending-cell claim and exact restoration transition scenarios.

use super::{
    PendingCellError, PendingSendCell, ProducerSendFailure, ProducerSendFailureKind,
    ProducerSendReadyFailure,
};
use crate::{ProducerSendError, ProducerSendStartFailure, ProducerSendStartFailureKind};

#[test]
fn settled_promotion_cannot_claim_the_cell_a_second_time() {
    let cell = PendingSendCell::new_for_test();
    let promotion = cell
        .begin_promotion()
        .unwrap_or_else(|error| panic!("pending cell should claim: {error:?}"));
    let job = promotion
        .settle_local(ProducerSendFailure::new(
            ProducerSendFailureKind::Backpressure,
        ))
        .unwrap_or_else(|(_promotion, error)| panic!("promotion should settle: {error:?}"));

    assert!(matches!(
        cell.begin_promotion(),
        Err(PendingCellError::AlreadySettled)
    ));
    job.dispatch_pending_notification_for_test();
}

#[test]
fn promotion_carries_start_failure_without_local_reclassification() {
    let cell = PendingSendCell::new_for_test();
    let send = crate::producer::boundary::ProducerSend::from_pending(cell.clone());
    let promotion = cell
        .begin_promotion()
        .unwrap_or_else(|error| panic!("pending cell should claim: {error:?}"));
    let failure =
        ProducerSendStartFailure::new(ProducerSendStartFailureKind::RecordSizeUnrepresentable);
    let job = promotion
        .settle_ready(ProducerSendReadyFailure::Start(failure))
        .unwrap_or_else(|(_promotion, error)| panic!("promotion should settle: {error:?}"));
    job.dispatch_pending_notification_for_test();

    assert_eq!(send.wait(), Err(ProducerSendError::Start(failure)));
}
