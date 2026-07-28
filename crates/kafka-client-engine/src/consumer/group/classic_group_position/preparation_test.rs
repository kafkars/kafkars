//! Position preparation ordering, fencing, and empty-assignment scenarios.

use std::sync::Arc;

use kafka_client_core::{
    AssignmentGeneration, Deadline, GroupAssignmentPartition, GroupId, GroupPositionBootstrapState,
    GroupPositionBootstrapTerminal, LiveGroupAssignment, MemberId, MembershipCycle, Moment,
    PartitionIndex, TopicId,
};

use crate::clock::OperationDeadline;

use super::{
    super::session_catalog::{GroupSessionCatalog, GroupSessionCatalogError},
    CLASSIC_GROUP_POSITION_REQUEST_RETAINED_BYTES, ClassicGroupPositionPreparation,
    ClassicGroupPositionPreparationError, prepare_classic_group_position,
};

#[test]
fn nonempty_preparation_preserves_core_order_fence_and_original_deadline() {
    let catalog = catalog(&["zeta", "alpha"]);
    let partitions = vec![partition(1, 2), partition(1, 7), partition(2, 1)];
    let assignment = assignment(catalog.group_id(), partitions.clone());
    let deadline = operation_deadline();
    let prepared = prepare_classic_group_position(
        &catalog,
        cycle(),
        &assignment,
        deadline,
        Moment::from_tick(5),
    )
    .unwrap_or_else(|error| panic!("position preparation: {error:?}"));
    let ClassicGroupPositionPreparation::Prepared(prepared) = prepared else {
        panic!("nonempty assignment must prepare one RPC");
    };
    assert_eq!(prepared.key().fence().group_id(), catalog.group_id());
    assert_eq!(prepared.key().fence().membership_cycle(), cycle());
    assert_eq!(prepared.key().fence().member_id(), assignment.member_id());
    assert_eq!(
        prepared.key().fence().assignment_generation(),
        assignment.assignment_generation()
    );
    assert_eq!(prepared.key().operation_deadline(), deadline);

    let (key, machine, correlation, request, result_buffer) = prepared.into_parts();
    assert_eq!(key.operation_deadline(), deadline);
    assert_eq!(machine.deadline(), deadline.core());
    assert_eq!(machine.state(), GroupPositionBootstrapState::AwaitingDriver);
    assert_eq!(machine.partitions(), partitions);
    assert_eq!(correlation.group_id(), "workers");
    assert_eq!(correlation.partition_count(), 3);
    assert_eq!(correlation.topics().len(), 2);
    assert_eq!(correlation.topics()[0].name(), "alpha");
    assert_eq!(correlation.topics()[0].partition_indexes(), &[2, 7]);
    assert_eq!(correlation.topics()[1].name(), "zeta");
    assert_eq!(correlation.topics()[1].partition_indexes(), &[1]);
    assert!(request.retained_bytes() <= CLASSIC_GROUP_POSITION_REQUEST_RETAINED_BYTES);
    assert!(result_buffer.is_empty());
    assert!(result_buffer.capacity() >= partitions.len());
}

#[test]
fn empty_assignment_completes_ready_without_rpc_ownership() {
    let catalog = catalog(&[]);
    let assignment = assignment(catalog.group_id(), Vec::new());
    let prepared = prepare_classic_group_position(
        &catalog,
        cycle(),
        &assignment,
        operation_deadline(),
        Moment::from_tick(5),
    )
    .unwrap_or_else(|error| panic!("empty position preparation: {error:?}"));
    let ClassicGroupPositionPreparation::Complete(completed) = prepared else {
        panic!("empty assignment must not create RPC ownership");
    };
    let (machine, terminal, observed_at, _operation_deadline) = completed.into_parts();
    assert_eq!(machine.state(), GroupPositionBootstrapState::Completed);
    assert_eq!(observed_at, Moment::from_tick(5));
    assert!(matches!(
        terminal,
        GroupPositionBootstrapTerminal::Ready(batch) if batch.facts().is_empty()
    ));
}

#[test]
fn local_catalog_failure_is_not_a_started_driver_rejection() {
    let catalog = catalog(&["orders"]);
    let unknown = TopicId::from_raw(7);
    let assignment = assignment(catalog.group_id(), vec![partition(unknown.get(), 0)]);
    assert_eq!(
        prepare_classic_group_position(
            &catalog,
            cycle(),
            &assignment,
            operation_deadline(),
            Moment::from_tick(5),
        )
        .err(),
        Some(ClassicGroupPositionPreparationError::UnknownTopic(
            GroupSessionCatalogError::UnknownTopic(unknown)
        ))
    );
}

#[test]
fn unrepresentable_partition_is_an_exact_local_preparation_error() {
    let catalog = catalog(&["orders"]);
    let partition = PartitionIndex::from_raw(
        u32::try_from(i32::MAX)
            .unwrap_or_else(|_| panic!("i32 max fits u32"))
            .saturating_add(1),
    );
    let assignment = LiveGroupAssignment::try_new(
        catalog.group_id(),
        member_id(),
        generation(),
        vec![GroupAssignmentPartition::new(
            TopicId::from_raw(1),
            partition,
        )],
    )
    .unwrap_or_else(|error| panic!("assignment: {error}"));
    assert_eq!(
        prepare_classic_group_position(
            &catalog,
            cycle(),
            &assignment,
            operation_deadline(),
            Moment::from_tick(5),
        )
        .err(),
        Some(ClassicGroupPositionPreparationError::PartitionOutOfRange(
            partition
        ))
    );
}

#[test]
fn mismatched_catalog_group_is_rejected_before_protocol_or_core_start() {
    let catalog = catalog(&["orders"]);
    let assignment_group = GroupId::try_from_raw(9).unwrap_or_else(|| panic!("assignment group"));
    let assignment = assignment(assignment_group, vec![partition(1, 0)]);
    assert_eq!(
        prepare_classic_group_position(
            &catalog,
            cycle(),
            &assignment,
            operation_deadline(),
            Moment::from_tick(5),
        )
        .err(),
        Some(ClassicGroupPositionPreparationError::CatalogGroup {
            catalog: catalog.group_id(),
            assignment: assignment_group,
        })
    );
}

fn catalog(topics: &[&str]) -> GroupSessionCatalog {
    let topics = topics.iter().copied().map(Arc::from).collect::<Vec<_>>();
    GroupSessionCatalog::try_new(group_id(), Arc::from("workers"), &topics)
        .unwrap_or_else(|error| panic!("catalog: {error:?}"))
}

fn assignment(group_id: GroupId, partitions: Vec<GroupAssignmentPartition>) -> LiveGroupAssignment {
    LiveGroupAssignment::try_new(group_id, member_id(), generation(), partitions)
        .unwrap_or_else(|error| panic!("assignment: {error}"))
}

fn partition(topic_id: u64, partition: u32) -> GroupAssignmentPartition {
    GroupAssignmentPartition::new(
        TopicId::from_raw(topic_id),
        PartitionIndex::from_raw(partition),
    )
}

fn group_id() -> GroupId {
    GroupId::try_from_raw(1).unwrap_or_else(|| panic!("group"))
}

fn member_id() -> MemberId {
    MemberId::try_from_raw(2).unwrap_or_else(|| panic!("member"))
}

fn generation() -> AssignmentGeneration {
    AssignmentGeneration::try_from_raw(3).unwrap_or_else(|| panic!("generation"))
}

fn cycle() -> MembershipCycle {
    MembershipCycle::try_from_raw(4).unwrap_or_else(|| panic!("cycle"))
}

fn operation_deadline() -> OperationDeadline {
    OperationDeadline::from_core_for_test(Deadline::from_tick(20))
}
