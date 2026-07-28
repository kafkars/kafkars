//! Exact lease and terminal identity evidence for graceful revocation.

use crate::{Deadline, consumer::AssignmentEpoch};

use super::{
    ClassicGracefulRevocationLease, ClassicGracefulRevocationLossReason,
    ClassicGracefulRevocationTerminal,
};

#[test]
fn lease_and_every_terminal_retain_exact_epoch_and_absolute_deadline() {
    let epoch = AssignmentEpoch::initial();
    let lease = ClassicGracefulRevocationLease::new(epoch, Deadline::from_tick(37));
    assert_eq!(lease.assignment_epoch(), epoch);
    assert_eq!(lease.deadline(), Deadline::from_tick(37));

    let acknowledged = ClassicGracefulRevocationTerminal::Acknowledged(lease);
    let lost = ClassicGracefulRevocationTerminal::Lost {
        lease,
        reason: ClassicGracefulRevocationLossReason::OwnerLost,
    };
    assert_eq!(acknowledged.lease(), lease);
    assert_eq!(lost.lease(), lease);
}
