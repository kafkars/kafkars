//! Locally unsubscribed retained-topic rejection through real catalog history.

use std::sync::Arc;

use kafka_client_core::{
    ClassicGeneration, ClassicGroupEffect, ClassicGroupInput, ClassicProtocol, GroupId,
    JoinedMemberSlot, Moment, TopicPartitionCount,
};

use crate::protocol::consumer::NamedAssignmentPartition;

use super::{
    classic_group_assignment_decode::{
        ClassicGroupAssignmentDecodeError, decode_classic_group_assignment,
    },
    classic_group_candidate::{ClassicGroupCycleCandidate, JoinedGroupMember},
    classic_group_owner::ClassicGroupOwner,
    classic_group_test_support,
    session_catalog::GroupSessionCatalog,
};

fn group_id() -> GroupId {
    GroupId::try_from_raw(31).unwrap_or_else(|| panic!("nonzero group identity"))
}

fn slot(raw: u32) -> JoinedMemberSlot {
    JoinedMemberSlot::try_from_raw(raw).unwrap_or_else(|| panic!("nonzero joined-member slot"))
}

fn follower_after_foreign_topic() -> (GroupSessionCatalog, ClassicGroupCycleCandidate) {
    let mut catalog =
        GroupSessionCatalog::try_new(group_id(), Arc::from("workers"), &[Arc::from("orders")])
            .unwrap_or_else(|error| panic!("catalog creation failed: {error:?}"));
    let mut owner = ClassicGroupOwner::new(
        group_id(),
        classic_group_test_support::timing(),
        classic_group_test_support::heartbeat_policy(),
        classic_group_test_support::rejoin_policy(),
    );
    let cycle = classic_group_test_support::begin(&mut owner);
    let candidate = catalog
        .prepare_leader_cycle(
            cycle,
            ClassicProtocol::Range,
            Arc::from("a-local"),
            vec![
                JoinedGroupMember::new(slot(1), Arc::from("a-local"), vec![Arc::from("orders")]),
                JoinedGroupMember::new(slot(2), Arc::from("z-remote"), vec![Arc::from("payments")]),
            ],
        )
        .unwrap_or_else(|error| panic!("leader candidate failed: {error:?}"));
    let member_id = candidate.local_member_id();
    let local_slot = candidate
        .local_slot()
        .unwrap_or_else(|| panic!("leader local slot"));
    let members = candidate
        .try_core_join_members()
        .unwrap_or_else(|error| panic!("core member translation failed: {error:?}"));
    let orders = catalog
        .topic_id("orders")
        .unwrap_or_else(|| panic!("orders identity"));
    let payments = catalog
        .next_topic_id
        .unwrap_or_else(|| panic!("staged payments identity"));
    owner
        .stage_candidate(candidate)
        .unwrap_or_else(|error| panic!("candidate stage failed: {error:?}"));
    owner
        .apply(ClassicGroupInput::JoinLeader {
            cycle,
            now: Moment::from_tick(2),
            member_id,
            local_slot,
            generation: ClassicGeneration::try_from_raw(7)
                .unwrap_or_else(|| panic!("classic generation")),
            members,
        })
        .unwrap_or_else(|error| panic!("leader Join failed: {error}"));
    owner
        .apply(ClassicGroupInput::PartitionCounts {
            cycle,
            now: Moment::from_tick(3),
            counts: vec![
                TopicPartitionCount::new(orders, 0),
                TopicPartitionCount::new(payments, 0),
            ],
        })
        .unwrap_or_else(|error| panic!("partition counts failed: {error}"));
    install_empty(&mut catalog, &mut owner, cycle);
    let follower_cycle = classic_group_test_support::begin(&mut owner);
    let follower = catalog
        .prepare_follower_cycle(follower_cycle, Arc::from("next-local"))
        .unwrap_or_else(|error| panic!("follower candidate failed: {error:?}"));
    (catalog, follower)
}

fn install_empty(
    catalog: &mut GroupSessionCatalog,
    owner: &mut ClassicGroupOwner,
    cycle: kafka_client_core::MembershipCycle,
) {
    let install = owner
        .apply(ClassicGroupInput::SyncSucceeded {
            cycle,
            now: Moment::from_tick(4),
            partitions: Vec::new(),
        })
        .unwrap_or_else(|error| panic!("leader Sync failed: {error}"))
        .into_effects()
        .next()
        .unwrap_or_else(|| panic!("install effect"));
    let ClassicGroupEffect::Install {
        assignment,
        classic_generation,
        heartbeat: _heartbeat,
    } = install
    else {
        panic!("install effect expected");
    };
    owner
        .prepare_install(catalog, assignment, classic_generation)
        .unwrap_or_else(|failure| panic!("install preparation failed: {:?}", failure.kind))
        .commit();
    let revoke = owner
        .apply(ClassicGroupInput::AssignmentLost { cycle })
        .unwrap_or_else(|error| panic!("assignment loss failed: {error}"))
        .into_effects()
        .next()
        .unwrap_or_else(|| panic!("revoke effect"));
    let ClassicGroupEffect::Revoke {
        assignment,
        classic_generation,
    } = revoke
    else {
        panic!("revoke effect expected");
    };
    owner
        .prepare_revoke(catalog, assignment, classic_generation)
        .unwrap_or_else(|failure| panic!("revoke preparation failed: {:?}", failure.kind))
        .commit();
}

#[test]
fn catalog_known_but_locally_unsubscribed_topic_is_rejected() {
    let (catalog, candidate) = follower_after_foreign_topic();
    let payments = catalog
        .topic_id("payments")
        .unwrap_or_else(|| panic!("retained foreign topic"));
    assert!(!candidate.local_owns_topic(payments));
    let named =
        NamedAssignmentPartition::from_assignment_decode_parts_for_test(Arc::from("payments"), 0);

    let failure = decode_classic_group_assignment(&catalog, &candidate, vec![named])
        .err()
        .unwrap_or_else(|| panic!("unsubscribed topic must reject"));

    assert_eq!(
        failure.kind(),
        ClassicGroupAssignmentDecodeError::UnsubscribedTopic {
            entry: 0,
            topic_id: payments,
        }
    );
    assert_eq!(failure.partitions()[0].topic(), "payments");
    assert!(catalog.live_assignment().is_none());
}
