//! Count-fence rejection and core-authorized leader Sync scenarios.

use std::{sync::Arc, time::Instant};

use kafka_client_core::{ClassicGeneration, Deadline, GroupId, JoinedMemberSlot, Moment};

use crate::clock::OperationDeadline;

use super::{
    classic_group_candidate::JoinedGroupMember,
    classic_group_execution::new_classic_group_execution,
    classic_group_join::ClassicGroupExecutionState, classic_group_owner::ClassicGroupOwner,
    classic_group_partition_counts::PreparedClassicGroupPartitionCounts,
    classic_group_test_support, session_catalog::GroupSessionCatalog,
};

#[test]
fn partition_counts_authorize_the_existing_sync_materialization() {
    let (catalog, mut owner, mut execution) = prepared_leader(4);

    execution
        .complete_partition_counts(&mut owner, &catalog, Moment::from_tick(3))
        .unwrap_or_else(|error| panic!("count application failed: {error:?}"));

    assert!(matches!(
        execution.borrow_execution_state(),
        ClassicGroupExecutionState::PreparedSync(prepared)
            if prepared.cycle() == owner.machine().active_cycle().unwrap_or_else(
                || panic!("active cycle")
            )
                && prepared.deadline().core() == Deadline::from_tick(100)
    ));
}

#[test]
fn rejected_partition_count_retains_the_exact_prepared_read() {
    let (catalog, mut owner, mut execution) = prepared_leader(u32::MAX);
    let cycle = execution
        .prepared_partition_counts()
        .unwrap_or_else(|| panic!("prepared counts expected"))
        .cycle();

    assert!(
        execution
            .complete_partition_counts(&mut owner, &catalog, Moment::from_tick(3))
            .is_err()
    );

    assert_eq!(
        execution
            .prepared_partition_counts()
            .map(PreparedClassicGroupPartitionCounts::cycle),
        Some(cycle)
    );
}

fn prepared_leader(
    partition_count: u32,
) -> (
    GroupSessionCatalog,
    ClassicGroupOwner,
    super::classic_group_execution::ClassicGroupExecution,
) {
    let group_id = GroupId::try_from_raw(31).unwrap_or_else(|| panic!("group identity"));
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
    let deadline = OperationDeadline::from_parts_for_test(Deadline::from_tick(100), Instant::now());
    let mut prepared = owner
        .apply_leader_join(
            candidate,
            ClassicGeneration::try_from_raw(7).unwrap_or_else(|| panic!("generation")),
            Moment::from_tick(2),
            deadline,
        )
        .unwrap_or_else(|error| panic!("leader Join failed: {error:?}"));
    prepared
        .append(orders, partition_count, 11)
        .unwrap_or_else(|error| panic!("partition count failed: {error:?}"));
    let mut execution = new_classic_group_execution();
    execution.set_execution_state(ClassicGroupExecutionState::PreparedPartitionCounts(
        prepared,
    ));
    (catalog, owner, execution)
}
