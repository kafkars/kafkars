//! Group and core fencing plus exact byte-lease reclamation scenarios.

use std::sync::Arc;

use kafka_client_core::{
    AssignmentGeneration, GroupAssignmentPartition, LiveGroupAssignment, Moment, PartitionIndex,
    TopicId,
};

use crate::clock::MonotonicClock;

use super::{
    super::session_catalog::{CurrentGroupSession, GroupSessionCatalog},
    ClassicGroupFetchDeliveryError, ClassicGroupFetchFront, ClassicGroupFetchOwner,
    ClassicGroupFetchReclaimError,
    test_support::{committed, completed_ready, install_ready_delivery_for_test, position_fence},
};

#[test]
fn exact_catalog_and_core_fences_transfer_one_ready_byte_lease() {
    let (mut owner, catalog, assignment) = ready_owner();
    assert_eq!(owner.fetches.retained().1, 1);

    let delivery = owner
        .take_delivery(&catalog)
        .unwrap_or_else(|error| panic!("take group delivery: {error:?}"))
        .unwrap_or_else(|| panic!("ready group delivery"));

    assert_eq!(delivery.group_id(), position_fence(7).group_id());
    assert_eq!(delivery.position_fence(), position_fence(7));
    assert_eq!(
        delivery.assignment_epoch(),
        owner
            .machine_assignment_epoch()
            .unwrap_or_else(|| panic!("assignment epoch"))
    );
    assert_eq!(delivery.topic(), "orders");
    assert_eq!(delivery.partition(), 1);
    assert_eq!(delivery.next_offset().get(), 20);
    assert_eq!(
        delivery
            .data_batches()
            .iter()
            .flat_map(|batch| &batch.records)
            .count(),
        3
    );
    assert_eq!(owner.fetches.retained().1, 1);

    owner
        .reclaim_delivery(delivery)
        .unwrap_or_else(|error| panic!("reclaim group delivery: {error:?}"));
    assert_eq!(owner.fetches.retained(), (0, 0, 0));
    assert_eq!(assignment.partitions().len(), 1);
}

#[test]
fn catalog_generation_mismatch_cannot_take_or_reorder_the_ready_lease() {
    let (mut owner, catalog, _assignment) = ready_owner();
    let mismatched = active_catalog(8);

    assert!(matches!(
        owner.take_delivery(&mismatched),
        Err(ClassicGroupFetchDeliveryError::CatalogAssignmentMismatch { .. })
    ));
    assert_eq!(owner.fetches.retained().1, 1);

    let delivery = owner
        .take_delivery(&catalog)
        .unwrap_or_else(|error| panic!("take exact delivery: {error:?}"))
        .unwrap_or_else(|| panic!("ready delivery remains FIFO"));
    owner
        .reclaim_delivery(delivery)
        .unwrap_or_else(|error| panic!("reclaim exact delivery: {error:?}"));
}

#[test]
fn authorization_effect_must_settle_before_the_lease_can_transfer() {
    let fence = position_fence(7);
    let catalog = active_catalog(7);
    let mut owner = activated_owner();
    assert_eq!(
        owner.interpret_front_effect(&catalog, &MonotonicClock::new()),
        ClassicGroupFetchFront::Interpreted
    );
    install_ready_terminal_without_authorization(&mut owner);

    assert_eq!(
        owner.take_delivery(&catalog).err(),
        Some(ClassicGroupFetchDeliveryError::EffectsPending)
    );
    assert_eq!(owner.fetches.retained().1, 1);
    assert_eq!(
        owner.interpret_front_effect(&catalog, &MonotonicClock::new()),
        ClassicGroupFetchFront::Interpreted
    );
    while !owner.effects.is_empty() {
        assert_eq!(
            owner.interpret_front_effect(&catalog, &MonotonicClock::new()),
            ClassicGroupFetchFront::Interpreted
        );
    }

    let delivery = owner
        .take_delivery(&catalog)
        .unwrap_or_else(|error| panic!("take authorized delivery: {error:?}"))
        .unwrap_or_else(|| panic!("authorized delivery"));
    assert_eq!(
        delivery.assignment_epoch(),
        owner
            .machine_assignment_epoch()
            .unwrap_or_else(|| panic!("active epoch"))
    );
    assert_eq!(delivery.position_fence(), fence);
    owner
        .reclaim_delivery(delivery)
        .unwrap_or_else(|error| panic!("reclaim delivery: {error:?}"));
}

#[test]
fn assignment_loss_does_not_prevent_exact_external_lease_reclaim() {
    let (mut owner, catalog, assignment) = ready_owner();
    let delivery = owner
        .take_delivery(&catalog)
        .unwrap_or_else(|error| panic!("take group delivery: {error:?}"))
        .unwrap_or_else(|| panic!("ready group delivery"));

    owner
        .retire_for_assignment_loss(&assignment)
        .unwrap_or_else(|error| panic!("retire assignment: {error:?}"));
    assert!(owner.activation().is_none());
    assert_eq!(owner.fetches.retained().1, 1);

    owner
        .reclaim_delivery(delivery)
        .unwrap_or_else(|error| panic!("reclaim after assignment loss: {error:?}"));
    assert_eq!(owner.fetches.retained(), (0, 0, 0));
}

#[test]
fn failed_reclaim_retains_the_exact_lease_until_post_driver_recovery() {
    let (mut owner, catalog, _assignment) = ready_owner();
    let delivery = owner
        .take_delivery(&catalog)
        .unwrap_or_else(|error| panic!("take group delivery: {error:?}"))
        .unwrap_or_else(|| panic!("ready group delivery"));
    owner.fetches.install_fault_for_test();

    assert_eq!(
        owner.reclaim_delivery(delivery),
        Err(ClassicGroupFetchReclaimError::Retained)
    );
    assert_eq!(owner.reclaim_faults.len(), 1);
    assert!(owner.reclaim_overflow.is_none());
    assert!(matches!(
        owner.take_delivery(&catalog),
        Err(ClassicGroupFetchDeliveryError::Faulted)
    ));

    let recovery = owner.release_after_driver_shutdown();
    assert_eq!(
        recovery.reclaim_fault(),
        Some(crate::consumer::fetch_execution::FetchExecutionError::Faulted)
    );
    assert_eq!(recovery.fetch_retained().1, 1);
    assert_eq!(recovery.reclaim_faults(), 1);
    assert!(!recovery.reclaim_overflow());
}

fn ready_owner() -> (
    ClassicGroupFetchOwner,
    GroupSessionCatalog,
    LiveGroupAssignment,
) {
    let catalog = active_catalog(7);
    let assignment = assignment(7);
    let mut owner = activated_owner();
    install_ready_delivery_for_test(&mut owner, &catalog, 17);
    assert!(owner.effects.is_empty());
    (owner, catalog, assignment)
}

fn activated_owner() -> ClassicGroupFetchOwner {
    let fence = position_fence(7);
    let mut owner =
        ClassicGroupFetchOwner::try_new().unwrap_or_else(|error| panic!("Fetch owner: {error:?}"));
    owner
        .try_activate(
            completed_ready(fence, Moment::from_tick(41), 0, vec![committed(1, 1, 17)]),
            fence,
        )
        .unwrap_or_else(|error| panic!("Fetch activation: {:?}", error.kind()));
    owner
}

fn install_ready_terminal_without_authorization(owner: &mut ClassicGroupFetchOwner) {
    use crate::{
        consumer::fetch_execution::{FetchTerminalFixture, install_terminal_for_test},
        protocol::fetch::fixture::encoded_delivery_batches_for_test,
    };

    let prepared = owner
        .pending_fetches
        .pop_front()
        .unwrap_or_else(|| panic!("prepared Fetch"));
    install_terminal_for_test(
        &mut owner.fetches,
        prepared,
        FetchTerminalFixture::Success(Some(encoded_delivery_batches_for_test(17))),
    );
    let transition = owner
        .fetches
        .poll(&mut owner.machine, Moment::from_tick(53))
        .unwrap_or_else(|error| panic!("Fetch settlement: {error:?}"))
        .unwrap_or_else(|| panic!("Fetch transition"));
    owner.effects.extend(transition.into_effects());
}

fn active_catalog(generation: u64) -> GroupSessionCatalog {
    let fence = position_fence(generation);
    let assignment = assignment(generation);
    let mut catalog = GroupSessionCatalog::try_new(
        fence.group_id(),
        Arc::from("workers"),
        &[Arc::from("orders")],
    )
    .unwrap_or_else(|error| panic!("catalog: {error:?}"));
    catalog.current = Some(CurrentGroupSession {
        member_id: fence.member_id(),
        member: Arc::from("member-a"),
        classic_generation: 3,
        assignment,
    });
    catalog
}

fn assignment(generation: u64) -> LiveGroupAssignment {
    let fence = position_fence(generation);
    LiveGroupAssignment::try_new(
        fence.group_id(),
        fence.member_id(),
        AssignmentGeneration::try_from_raw(generation)
            .unwrap_or_else(|| panic!("assignment generation")),
        vec![GroupAssignmentPartition::new(
            TopicId::from_raw(1),
            PartitionIndex::from_raw(1),
        )],
    )
    .unwrap_or_else(|error| panic!("live assignment: {error:?}"))
}
