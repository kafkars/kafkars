//! Classic membership identity direction and domain evidence.

use super::{ClassicGeneration, JoinedMemberSlot, MemberRank, MembershipCycle};

#[test]
fn membership_cycle_is_nonzero_directional_and_never_wraps() {
    let first = MembershipCycle::initial();
    assert_eq!(first.get(), 1);
    assert_eq!(first.checked_next().map(MembershipCycle::get), Some(2));
    assert_eq!(MembershipCycle::try_from_raw(0), None);
    assert_eq!(
        MembershipCycle::try_from_raw(u64::MAX).and_then(MembershipCycle::checked_next),
        None
    );
}

#[test]
fn join_slots_and_member_ranks_are_nonzero() {
    assert_eq!(JoinedMemberSlot::try_from_raw(0), None);
    assert_eq!(MemberRank::try_from_raw(0), None);
    assert_eq!(
        JoinedMemberSlot::try_from_raw(3).map(JoinedMemberSlot::get),
        Some(3)
    );
    assert_eq!(MemberRank::try_from_raw(5).map(MemberRank::get), Some(5));
}

#[test]
fn classic_generation_preserves_kafkas_nonnegative_signed_domain() {
    assert_eq!(ClassicGeneration::try_from_raw(-1), None);
    assert_eq!(
        ClassicGeneration::try_from_raw(0).map(ClassicGeneration::get),
        Some(0)
    );
    assert_eq!(
        ClassicGeneration::try_from_raw(i32::MAX).map(ClassicGeneration::get),
        Some(i32::MAX)
    );
}
