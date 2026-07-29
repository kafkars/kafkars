//! Whole-operation failure fact scenarios.

use crate::DeliveryStatus;

use super::{AdminListTransactionsFailure, AdminListTransactionsFailureKind};

#[test]
fn failure_preserves_kind_and_delivery_certainty() {
    let failure = AdminListTransactionsFailure::new(
        AdminListTransactionsFailureKind::Transport,
        DeliveryStatus::PossiblySent,
    );
    assert_eq!(failure.kind(), AdminListTransactionsFailureKind::Transport);
    assert_eq!(failure.delivery(), DeliveryStatus::PossiblySent);
}
