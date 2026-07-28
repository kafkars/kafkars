//! Exact `TxnOffsetCommit` target borrowing scenarios.

use std::sync::Arc;

use super::offset_commit_target::{TransactionOffsetCommitTarget, target_refs};

#[test]
fn target_refs_preserve_caller_order_and_every_offset_fact() {
    let targets = vec![
        TransactionOffsetCommitTarget::new(
            Arc::from("orders"),
            2,
            93,
            Some(7),
            Some(Arc::from("checkpoint-a")),
        ),
        TransactionOffsetCommitTarget::new(Arc::from("audit"), 1, 12, None, None),
    ];
    let refs = target_refs(&targets).unwrap_or_else(|| panic!("bounded target refs"));
    assert_eq!(
        (
            refs[0].topic(),
            refs[0].partition(),
            refs[0].next_offset(),
            refs[0].leader_epoch(),
            refs[0].metadata(),
        ),
        ("orders", 2, 93, Some(7), Some("checkpoint-a"))
    );
    assert_eq!(
        (
            refs[1].topic(),
            refs[1].partition(),
            refs[1].next_offset(),
            refs[1].leader_epoch(),
            refs[1].metadata(),
        ),
        ("audit", 1, 12, None, None)
    );
}
