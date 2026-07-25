//! Exact identity and linear-ownership contracts for one prepared classic Sync.

use std::time::{Duration, Instant};

use kafka_client_core::{ClassicGeneration, Deadline, GroupId, MemberId, MembershipCycle};

use crate::{clock::OperationDeadline, protocol::consumer::classic_follower_sync_group_request};

use super::classic_group_sync::{
    ClassicGroupSyncDriverOwner, ClassicGroupSyncIdentity, PreparedClassicGroupSync,
};

macro_rules! assert_not_impl {
    ($type:ty: $trait:path) => {
        const _: fn() = || {
            struct Implemented;
            trait AmbiguousIfImplemented<A> {
                fn check() {}
            }
            impl<T: ?Sized> AmbiguousIfImplemented<()> for T {}
            impl<T: ?Sized + $trait> AmbiguousIfImplemented<Implemented> for T {}
            let _ = <$type as AmbiguousIfImplemented<_>>::check;
        };
    };
}

#[test]
fn prepared_sync_preserves_every_core_and_transport_fence() {
    let group_id = GroupId::try_from_raw(7).unwrap_or_else(|| panic!("nonzero group identity"));
    let cycle = MembershipCycle::try_from_raw(11).unwrap_or_else(|| panic!("nonzero cycle"));
    let member_id = MemberId::try_from_raw(13).unwrap_or_else(|| panic!("nonzero member identity"));
    let generation = ClassicGeneration::try_from_raw(17)
        .unwrap_or_else(|| panic!("valid signed classic generation"));
    let transport = Instant::now() + Duration::from_secs(2);
    let deadline = OperationDeadline::from_parts_for_test(Deadline::from_tick(23), transport);
    let identity = ClassicGroupSyncIdentity::new(group_id, cycle, member_id, generation, deadline);
    let request = classic_follower_sync_group_request("workers", "member-a", generation)
        .unwrap_or_else(|error| panic!("valid empty follower Sync: {error:?}"));
    let prepared = PreparedClassicGroupSync::new(identity, request);

    assert_eq!(prepared.identity(), identity);
    assert_eq!(prepared.group_id(), group_id);
    assert_eq!(prepared.cycle(), cycle);
    assert_eq!(prepared.member_id(), member_id);
    assert_eq!(prepared.generation(), generation);
    assert_eq!(prepared.deadline().core(), Deadline::from_tick(23));
    assert_eq!(prepared.deadline().transport(), transport);

    let (parts_identity, request) = prepared.into_parts();
    assert_eq!(parts_identity, identity);
    drop(request);
}

#[test]
fn sync_identity_is_copy_but_ownership_values_are_linear() {
    fn require_copy<T: Copy>() {}

    require_copy::<ClassicGroupSyncIdentity>();
    assert_not_impl!(PreparedClassicGroupSync: Clone);
    assert_not_impl!(PreparedClassicGroupSync: Copy);
    assert_not_impl!(ClassicGroupSyncDriverOwner: Clone);
    assert_not_impl!(ClassicGroupSyncDriverOwner: Copy);
}
