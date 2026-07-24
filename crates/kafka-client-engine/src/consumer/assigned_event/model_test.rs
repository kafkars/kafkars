//! Exact scalar recovery observations for bounded assigned events.

use super::model::AssignedConsumerEventRecovery;

#[test]
fn recovery_reports_claimed_and_ready_ownership_separately() {
    let recovery = AssignedConsumerEventRecovery::new(3, 2);

    assert_eq!(recovery.claimed(), 3);
    assert_eq!(recovery.ready(), 2);
}
