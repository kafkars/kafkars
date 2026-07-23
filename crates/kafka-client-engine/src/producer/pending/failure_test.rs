//! Local pending-send failure vocabulary scenarios.

use super::{ProducerSendFailure, ProducerSendFailureKind};
use crate::ProducerDeliveryStatus;

#[test]
fn every_local_failure_is_explicitly_not_sent() {
    for kind in [
        ProducerSendFailureKind::DeadlineElapsed,
        ProducerSendFailureKind::Shutdown,
        ProducerSendFailureKind::Closed,
        ProducerSendFailureKind::Backpressure,
        ProducerSendFailureKind::Cancelled,
    ] {
        let failure = ProducerSendFailure::new(kind);
        assert_eq!(failure.kind(), kind);
        assert_eq!(failure.delivery_status(), ProducerDeliveryStatus::NotSent);
    }
}
