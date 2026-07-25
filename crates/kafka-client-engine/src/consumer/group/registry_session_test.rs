//! Registry-selected owned membership-cycle staging and close fencing.

use std::sync::Arc;

use kafka_client_core::{ClassicGroupInput, Deadline, GroupId, JoinedMemberSlot, Moment};

use super::{
    classic_group_candidate::JoinedGroupMember,
    registry_close::GroupConsumerCloseError,
    registry_session::GroupConsumerSessionFailure,
    registry_test_support::{register, started_registry, stop_registry},
};

#[test]
fn follower_candidate_is_selected_by_nonreused_group_identity() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    let cycle = begin(&mut registry, group_id);

    let member_id = registry
        .stage_follower_cycle(group_id, cycle, Arc::from("member"))
        .unwrap_or_else(|error| panic!("follower staging failed: {error:?}"));
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("registered entry expected"));
    assert_eq!(
        entry
            .classic
            .pending()
            .map(super::classic_group_candidate::ClassicGroupCycleCandidate::local_member_id),
        Some(member_id)
    );
    assert_eq!(
        entry
            .catalog
            .next_member_id
            .map(kafka_client_core::MemberId::get),
        Some(1)
    );
    stop_registry(&mut registry);
}

#[test]
fn leader_candidate_returns_ranked_core_members_and_stages_the_exact_owner() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    let cycle = begin(&mut registry, group_id);
    let local_slot = JoinedMemberSlot::try_from_raw(2)
        .unwrap_or_else(|| panic!("joined-member slot must be nonzero"));
    let remote_slot = JoinedMemberSlot::try_from_raw(1)
        .unwrap_or_else(|| panic!("joined-member slot must be nonzero"));

    let staged = registry
        .stage_leader_cycle(
            group_id,
            cycle,
            Arc::from("a-local"),
            vec![
                JoinedGroupMember::new(
                    remote_slot,
                    Arc::from("z-remote"),
                    vec![Arc::from("payments")],
                ),
                JoinedGroupMember::new(local_slot, Arc::from("a-local"), vec![Arc::from("orders")]),
            ],
        )
        .unwrap_or_else(|error| panic!("leader staging failed: {error:?}"));

    assert_eq!(staged.local_slot, local_slot);
    assert_eq!(staged.members.members().len(), 2);
    assert_eq!(staged.members.members()[0].slot(), local_slot);
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("registered entry expected"));
    assert_eq!(
        entry
            .classic
            .pending()
            .map(super::classic_group_candidate::ClassicGroupCycleCandidate::local_member_id),
        Some(staged.member_id)
    );
    assert_eq!(entry.catalog.topic_id("payments"), None);
    stop_registry(&mut registry);
}

#[test]
fn unknown_and_closing_groups_cannot_stage_membership() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    let cycle = begin(&mut registry, group_id);
    let unknown =
        GroupId::try_from_raw(999).unwrap_or_else(|| panic!("unknown identity must be nonzero"));

    assert_eq!(
        registry
            .stage_follower_cycle(unknown, cycle, Arc::from("member"))
            .err(),
        Some(GroupConsumerSessionFailure::UnknownGroup)
    );
    assert_eq!(registry.close_group(group_id), Ok(()));
    assert_eq!(
        registry.close_group(group_id),
        Err(GroupConsumerCloseError::AlreadyClosing)
    );
    assert_eq!(
        registry
            .stage_follower_cycle(group_id, cycle, Arc::from("member"))
            .err(),
        Some(GroupConsumerSessionFailure::Closing)
    );
    stop_registry(&mut registry);
}

fn begin(
    registry: &mut super::registry::GroupConsumerRegistry,
    group_id: GroupId,
) -> kafka_client_core::MembershipCycle {
    let entry = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("registered entry expected"));
    entry
        .classic
        .apply(ClassicGroupInput::Begin {
            now: Moment::from_tick(1),
            deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("membership begin failed: {error}"));
    entry
        .classic
        .machine()
        .active_cycle()
        .unwrap_or_else(|| panic!("active cycle expected"))
}
