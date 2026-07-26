//! Atomic leader candidate and exact count-effect ownership scenarios.

use std::{sync::Arc, time::Instant};

use kafka_client_core::{
    ClassicGeneration, ClassicGroupPhase, Deadline, GroupId, JoinedMemberSlot, Moment,
};

use crate::clock::OperationDeadline;

use super::{
    classic_group_candidate::JoinedGroupMember, classic_group_owner::ClassicGroupOwner,
    classic_group_test_support, session_catalog::GroupSessionCatalog,
};

#[test]
fn leader_join_stages_candidate_and_exact_count_request() {
    let group_id = GroupId::try_from_raw(19).unwrap_or_else(|| panic!("group identity"));
    let catalog =
        GroupSessionCatalog::try_new(group_id, Arc::from("workers"), &[Arc::from("orders")])
            .unwrap_or_else(|error| panic!("catalog creation failed: {error:?}"));
    let orders = catalog
        .topic_id("orders")
        .unwrap_or_else(|| panic!("orders topic identity"));
    let mut owner = ClassicGroupOwner::new(
        group_id,
        classic_group_test_support::timing(),
        classic_group_test_support::heartbeat_policy(),
        classic_group_test_support::rejoin_policy(),
    );
    let cycle = classic_group_test_support::begin(&mut owner);
    let candidate = catalog
        .prepare_leader_cycle(
            cycle,
            Arc::from("member-a"),
            vec![JoinedGroupMember::new(
                JoinedMemberSlot::try_from_raw(1).unwrap_or_else(|| panic!("member slot")),
                Arc::from("member-a"),
                vec![Arc::from("orders")],
            )],
        )
        .unwrap_or_else(|error| panic!("candidate failed: {error:?}"));
    let transport = Instant::now();
    let deadline = OperationDeadline::from_parts_for_test(Deadline::from_tick(100), transport);

    let prepared = owner
        .apply_leader_join(
            candidate,
            ClassicGeneration::try_from_raw(7).unwrap_or_else(|| panic!("generation")),
            Moment::from_tick(2),
            deadline,
        )
        .unwrap_or_else(|error| panic!("leader Join failed: {error:?}"));

    assert_eq!(prepared.cycle(), cycle);
    assert_eq!(prepared.topics(), &[orders]);
    assert_eq!(prepared.deadline().core(), Deadline::from_tick(100));
    assert_eq!(prepared.deadline().transport(), transport);
    assert_eq!(
        owner.machine().phase(),
        ClassicGroupPhase::AwaitingPartitionCounts
    );
    assert!(owner.pending().is_some());
}
