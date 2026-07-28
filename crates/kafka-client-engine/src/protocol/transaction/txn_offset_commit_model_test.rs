//! Transactional group and next-offset borrowed-model scenarios.

use super::{TransactionGroupIdentityRef, TransactionOffsetCommitRef};

#[test]
fn group_identity_retains_every_membership_scalar() {
    let group = TransactionGroupIdentityRef::new("invoices", 17, "member-a", Some("instance-a"));

    assert_eq!(group.group_id(), "invoices");
    assert_eq!(group.generation_id_or_member_epoch(), 17);
    assert_eq!(group.member_id(), "member-a");
    assert_eq!(group.group_instance_id(), Some("instance-a"));
}

#[test]
fn offset_retains_next_position_and_nullable_checkpoint_facts() {
    let present = TransactionOffsetCommitRef::new("orders", 2, 93, Some(7), Some("checkpoint-a"));
    assert_eq!(present.topic(), "orders");
    assert_eq!(present.partition(), 2);
    assert_eq!(present.next_offset(), 93);
    assert_eq!(present.leader_epoch(), Some(7));
    assert_eq!(present.metadata(), Some("checkpoint-a"));

    let absent = TransactionOffsetCommitRef::new("audit", 1, 12, None, None);
    assert_eq!(absent.leader_epoch(), None);
    assert_eq!(absent.metadata(), None);
}
