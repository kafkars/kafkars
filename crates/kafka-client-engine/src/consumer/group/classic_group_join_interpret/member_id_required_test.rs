//! Static `MEMBER_ID_REQUIRED` settlement and same-deadline replacement evidence.

use std::{sync::Arc, time::Duration};

use kafka_client_core::{GroupId, Moment};

use crate::{
    clock::MonotonicClock,
    consumer::group::{
        classic_group_join::{
            ClassicGroupExecutionState, ClassicGroupJoinIdentity, ClassicGroupJoinSuccessor,
        },
        classic_group_join_settlement::ClassicGroupJoinSettlementTurn,
        registry::GroupConsumerRegistry,
        registry_entry::default_classic_processing_lease_policy,
        registry_test_support::{started_registry, stop_registry},
    },
    driver::classic_group::{
        AcceptedJoinGroupCall, JoinGroupCallKey, install_member_id_required_join_terminal,
    },
};

#[test]
fn static_member_id_required_stages_one_same_cycle_same_deadline_replacement_join() {
    let (mut registry, group_id, identity) = prepared_static_join_terminal();
    install_member_id_required_join_terminal(
        registry
            .join_calls
            .as_mut()
            .unwrap_or_else(|| panic!("Join calls expected")),
        join_key(identity),
        "assigned-member",
    );

    assert_eq!(
        registry.settle_one_classic_join(Moment::from_tick(1)),
        Ok(ClassicGroupJoinSettlementTurn::Progress)
    );
    let staged = entry(&registry, group_id);
    let required = staged
        .catalog
        .required_join_member
        .as_ref()
        .unwrap_or_else(|| panic!("broker-assigned member must be retained"));
    assert_eq!(required.cycle, identity.cycle());
    assert_eq!(required.member.as_ref(), "assigned-member");
    let ClassicGroupExecutionState::JoinConfirmationPending {
        call,
        successor: ClassicGroupJoinSuccessor::Join(prepared),
    } = staged.execution.borrow_execution_state()
    else {
        panic!("required-member Join confirmation");
    };
    assert_eq!(call.identity(), identity);
    assert_eq!(prepared.cycle(), identity.cycle());
    assert_eq!(prepared.deadline(), identity.deadline());
    assert_eq!(prepared.member_id(), Some(required.member_id));

    assert_eq!(
        registry.settle_one_classic_join(Moment::from_tick(2)),
        Ok(ClassicGroupJoinSettlementTurn::Progress)
    );
    let prepared = entry(&registry, group_id);
    let ClassicGroupExecutionState::PreparedJoin(join) =
        prepared.execution.borrow_execution_state()
    else {
        panic!("required-member Join preparation");
    };
    assert_eq!(join.cycle(), identity.cycle());
    assert_eq!(join.deadline(), identity.deadline());
    assert!(join.member_id().is_some());
    stop_registry(&mut registry);
}

fn prepared_static_join_terminal() -> (GroupConsumerRegistry, GroupId, ClassicGroupJoinIdentity) {
    let mut registry = started_registry();
    let group_id = registry
        .try_register_with_configuration(
            Arc::from("workers"),
            Some(Arc::from("instance-a")),
            vec![Arc::from("orders")],
            crate::consumer::group::classic_group_test_support::timing(),
            crate::consumer::group::classic_group_test_support::heartbeat_policy(),
            crate::consumer::group::classic_group_test_support::rejoin_policy(),
            kafka_client_core::GroupPositionMissingOffsetPolicy::Error,
            kafka_client_core::ReadIsolation::ReadUncommitted,
            default_classic_processing_lease_policy(),
        )
        .unwrap_or_else(|failure| panic!("static registration failed: {:?}", failure.kind));
    let entry = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("registered entry expected"));
    let capture = MonotonicClock::new()
        .capture_deadline_after(Duration::from_secs(7))
        .unwrap_or_else(|error| panic!("deadline capture failed: {error}"));
    entry
        .execution
        .begin(&mut entry.classic, capture)
        .unwrap_or_else(|error| panic!("classic begin failed: {error:?}"));
    let handoff = entry
        .execution
        .begin_join_handoff()
        .unwrap_or_else(|error| panic!("Join handoff failed: {error:?}"));
    let identity = handoff.identity();
    entry
        .execution
        .confirm_join_driver_owned(
            handoff.into_driver_acceptance(),
            AcceptedJoinGroupCall::from_key_for_test(join_key(identity)),
        )
        .unwrap_or_else(|_failure| panic!("Join ownership failed"));
    (registry, group_id, identity)
}

fn entry(
    registry: &GroupConsumerRegistry,
    group_id: GroupId,
) -> &crate::consumer::group::registry_entry::GroupConsumerEntry {
    registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("registered entry expected"))
}

fn join_key(identity: ClassicGroupJoinIdentity) -> JoinGroupCallKey {
    JoinGroupCallKey::new(identity.group_id(), identity.cycle(), identity.deadline())
}
