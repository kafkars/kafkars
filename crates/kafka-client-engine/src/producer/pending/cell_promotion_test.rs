//! Pending-cell claim and exact restoration transition scenarios.

use super::{PendingCellError, PendingSendCell, ProducerSendFailure, ProducerSendFailureKind};

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
