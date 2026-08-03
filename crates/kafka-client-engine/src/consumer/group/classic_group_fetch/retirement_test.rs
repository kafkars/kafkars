//! Exact Fetch retirement ordering and assignment-loss identity fencing.

use kafka_client_core::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignmentGeneration, FetchFailure,
    FetchRecords, FetchThrottleFailure, GroupAssignmentPartition, LiveGroupAssignment, Moment,
    NextFetchOffset, PartitionIndex, TopicId,
};

use crate::{
    consumer::fetch_execution::{FetchTerminalFixture, install_terminal_for_test},
    protocol::fetch::fixture::encoded_data_batch_for_test,
};

use super::{
    ClassicGroupFetchFront, ClassicGroupFetchOwner, ClassicGroupFetchOwnerFaultKind,
    ClassicGroupFetchRetirement, ClassicGroupFetchRetirementError,
    model::ClassicGroupFetchTransitionFailure,
    test_support::{catalog, committed, completed_ready, position_fence},
};

#[test]
fn exact_loss_retires_before_catalog_revocation_and_queues_ordered_controls() {
    let fence = position_fence(7);
    let assignment = assignment(7, &[(2, 1), (3, 0)]);
    let mut owner =
        ClassicGroupFetchOwner::try_new().unwrap_or_else(|error| panic!("Fetch owner: {error:?}"));
    owner
        .try_activate(
            completed_ready(
                fence,
                Moment::from_tick(41),
                0,
                vec![committed(2, 1, 17), committed(3, 0, 23)],
            ),
            fence,
        )
        .unwrap_or_else(|error| panic!("Fetch activation: {:?}", error.kind()));
    let assignment_epoch = owner
        .machine_assignment_epoch()
        .unwrap_or_else(|| panic!("active assignment epoch"));

    let retirement = owner
        .retire_for_assignment_loss(&assignment)
        .unwrap_or_else(|error| panic!("Fetch retirement: {error:?}"));

    assert_eq!(
        retirement,
        ClassicGroupFetchRetirement::Retired {
            position_fence: fence,
            assignment_epoch,
            controls: 2,
        }
    );
    assert!(owner.activation().is_none());
    assert_eq!(owner.machine_assignment_epoch(), None);
    let effects = owner.effects.iter().copied().collect::<Vec<_>>();
    assert_eq!(effects.len(), 4);
    assert!(matches!(
        effects[0],
        AssignedConsumerEffect::FetchReady { .. }
    ));
    assert!(matches!(
        effects[1],
        AssignedConsumerEffect::FetchReady { .. }
    ));
    assert_eq!(
        effects[2],
        AssignedConsumerEffect::Revoke {
            assignment_epoch,
            partition: kafka_client_core::AssignedTopicPartition::new(
                TopicId::from_raw(2),
                PartitionIndex::from_raw(1),
            ),
        }
    );
    assert_eq!(
        effects[3],
        AssignedConsumerEffect::Revoke {
            assignment_epoch,
            partition: kafka_client_core::AssignedTopicPartition::new(
                TopicId::from_raw(3),
                PartitionIndex::from_raw(0),
            ),
        }
    );
}

#[test]
fn foreign_membership_assignment_cannot_retire_fetch_or_mutate_queues() {
    let fence = position_fence(7);
    let mut owner =
        ClassicGroupFetchOwner::try_new().unwrap_or_else(|error| panic!("Fetch owner: {error:?}"));
    owner
        .try_activate(
            completed_ready(fence, Moment::from_tick(41), 0, vec![committed(2, 1, 17)]),
            fence,
        )
        .unwrap_or_else(|error| panic!("Fetch activation: {:?}", error.kind()));
    let epoch = owner.machine_assignment_epoch();
    let effects = owner.effects.len();

    assert!(matches!(
        owner.retire_for_assignment_loss(&assignment(8, &[(2, 1)])),
        Err(ClassicGroupFetchRetirementError::AssignmentIdentityMismatch { .. })
    ));
    assert_eq!(owner.machine_assignment_epoch(), epoch);
    assert_eq!(owner.effects.len(), effects);
    assert!(owner.activation().is_some());
    assert!(owner.fault().is_none());
}

#[test]
fn changed_partition_vector_retains_the_post_core_transition_without_catalog_revoke() {
    let fence = position_fence(7);
    let mut owner =
        ClassicGroupFetchOwner::try_new().unwrap_or_else(|error| panic!("Fetch owner: {error:?}"));
    owner
        .try_activate(
            completed_ready(fence, Moment::from_tick(41), 0, vec![committed(2, 1, 17)]),
            fence,
        )
        .unwrap_or_else(|error| panic!("Fetch activation: {:?}", error.kind()));

    assert_eq!(
        owner.retire_for_assignment_loss(&assignment(7, &[(3, 0)])),
        Err(ClassicGroupFetchRetirementError::Retained(
            ClassicGroupFetchOwnerFaultKind::Transition(
                ClassicGroupFetchTransitionFailure::RetirementControls,
            )
        ))
    );
    assert_eq!(owner.machine_assignment_epoch(), None);
    assert!(owner.activation().is_some());
    assert_eq!(
        owner
            .fault()
            .map(super::model::ClassicGroupFetchOwnerFault::kind),
        Some(ClassicGroupFetchOwnerFaultKind::Transition(
            ClassicGroupFetchTransitionFailure::RetirementControls,
        ))
    );
}

#[test]
fn loss_before_position_activation_is_an_allocation_free_noop() {
    let mut owner =
        ClassicGroupFetchOwner::try_new().unwrap_or_else(|error| panic!("Fetch owner: {error:?}"));

    assert_eq!(
        owner
            .retire_for_assignment_loss(&assignment(7, &[(2, 1)]))
            .unwrap_or_else(|error| panic!("inactive retirement: {error:?}")),
        ClassicGroupFetchRetirement::Inactive
    );
    assert_eq!(owner.unsettled(), 0);
}

#[test]
fn assignment_loss_reclaims_the_one_ready_delivery_before_quiescence() {
    let fence = position_fence(7);
    let assignment = assignment(7, &[(2, 1)]);
    let mut owner =
        ClassicGroupFetchOwner::try_new().unwrap_or_else(|error| panic!("Fetch owner: {error:?}"));
    owner
        .try_activate(
            completed_ready(fence, Moment::from_tick(41), 0, vec![committed(2, 1, 17)]),
            fence,
        )
        .unwrap_or_else(|error| panic!("Fetch activation: {:?}", error.kind()));
    assert_eq!(
        owner.interpret_front_effect(
            &super::test_support::catalog(&["orders", "payments"]),
            &crate::clock::MonotonicClock::new(),
        ),
        super::ClassicGroupFetchFront::Interpreted
    );
    let prepared = owner
        .pending_fetches
        .pop_front()
        .unwrap_or_else(|| panic!("prepared Fetch"));
    install_terminal_for_test(
        &mut owner.fetches,
        prepared,
        FetchTerminalFixture::Success(Some(encoded_data_batch_for_test(17))),
    );
    let transition = owner
        .fetches
        .poll(&mut owner.machine, Moment::from_tick(53))
        .unwrap_or_else(|error| panic!("Fetch settlement: {error:?}"))
        .unwrap_or_else(|| panic!("Fetch transition"));
    owner.effects.extend(transition.into_effects());
    assert_eq!(owner.fetches.retained().1, 1);

    owner
        .retire_for_assignment_loss(&assignment)
        .unwrap_or_else(|error| panic!("Fetch retirement: {error:?}"));

    assert_eq!(owner.fetches.retained(), (0, 0, 0));
    assert!(owner.activation().is_none());
    assert!(owner.fault().is_none());
}

#[test]
fn exact_assignment_loss_discards_a_retired_fetch_failure_before_revoke() {
    assert_retired_terminal_drains(RetiredTerminal::FetchFailed);
}

#[test]
fn exact_assignment_loss_discards_a_retired_fetch_throttle_failure_before_revoke() {
    assert_retired_terminal_drains(RetiredTerminal::FetchThrottleFailed);
}

#[derive(Clone, Copy)]
enum RetiredTerminal {
    FetchFailed,
    FetchThrottleFailed,
}

fn assert_retired_terminal_drains(terminal: RetiredTerminal) {
    let group_fence = position_fence(7);
    let assignment = assignment(7, &[(2, 1)]);
    let catalog = catalog(&["orders", "payments"]);
    let clock = crate::clock::MonotonicClock::new();
    let mut owner =
        ClassicGroupFetchOwner::try_new().unwrap_or_else(|error| panic!("Fetch owner: {error:?}"));
    owner
        .try_activate(
            completed_ready(
                group_fence,
                Moment::from_tick(41),
                0,
                vec![committed(2, 1, 17)],
            ),
            group_fence,
        )
        .unwrap_or_else(|error| panic!("Fetch activation: {:?}", error.kind()));
    let fetch_fence = match owner.effects.front().copied() {
        Some(AssignedConsumerEffect::FetchReady { fence, .. }) => fence,
        effect => panic!("initial FetchReady, got {effect:?}"),
    };
    assert_eq!(
        owner.interpret_front_effect(&catalog, &clock),
        ClassicGroupFetchFront::Interpreted
    );

    let transition = match terminal {
        RetiredTerminal::FetchFailed => owner.machine.apply(AssignedConsumerInput::FetchFailed {
            fence: fetch_fence,
            failure: FetchFailure::Transport,
        }),
        RetiredTerminal::FetchThrottleFailed => {
            owner.machine.apply(AssignedConsumerInput::FetchAdvanced {
                fence: fetch_fence,
                records: FetchRecords::NoApplicationRecords,
                next_offset: NextFetchOffset::try_from_raw(18)
                    .unwrap_or_else(|| panic!("next offset")),
                now: Moment::from_tick(u64::MAX - 1),
                throttle_ticks: 2,
            })
        }
    }
    .unwrap_or_else(|error| panic!("terminal Fetch transition: {error}"));
    owner.effects.extend(transition.into_effects());
    let retained = owner
        .effects
        .front()
        .copied()
        .unwrap_or_else(|| panic!("terminal Fetch effect"));
    assert!(
        matches!(
            (terminal, retained),
            (
                RetiredTerminal::FetchFailed,
                AssignedConsumerEffect::FetchFailed {
                    fence,
                    failure: FetchFailure::Transport,
                }
            ) if fence == fetch_fence
        ) || matches!(
            (terminal, retained),
            (
                RetiredTerminal::FetchThrottleFailed,
                AssignedConsumerEffect::FetchThrottleFailed {
                    fence,
                    failure: FetchThrottleFailure::DeadlineOverflow,
                }
            ) if fence == fetch_fence
        )
    );

    let assignment_epoch = owner
        .machine_assignment_epoch()
        .unwrap_or_else(|| panic!("active assignment epoch"));
    owner
        .retire_for_assignment_loss(&assignment)
        .unwrap_or_else(|error| panic!("Fetch retirement: {error:?}"));
    assert_eq!(owner.effects.front().copied(), Some(retained));
    assert_eq!(
        owner.effects.get(1).copied(),
        Some(AssignedConsumerEffect::Revoke {
            assignment_epoch,
            partition: fetch_fence.position().partition(),
        })
    );

    assert_eq!(
        owner.interpret_front_effect(&catalog, &clock),
        ClassicGroupFetchFront::Interpreted
    );
    assert_eq!(owner.events.retained(), (0, 0));
    assert_eq!(
        owner.interpret_front_effect(&catalog, &clock),
        ClassicGroupFetchFront::Interpreted
    );
    assert!(owner.effects.is_empty());
    assert!(owner.pending_fetches.is_empty());
    assert!(owner.is_idle());
    assert!(owner.fault().is_none());
}

fn assignment(generation: u64, partitions: &[(u64, u32)]) -> LiveGroupAssignment {
    let fence = position_fence(generation);
    LiveGroupAssignment::try_new(
        fence.group_id(),
        fence.member_id(),
        AssignmentGeneration::try_from_raw(generation)
            .unwrap_or_else(|| panic!("assignment generation")),
        partitions
            .iter()
            .map(|(topic, partition)| {
                GroupAssignmentPartition::new(
                    TopicId::from_raw(*topic),
                    PartitionIndex::from_raw(*partition),
                )
            })
            .collect(),
    )
    .unwrap_or_else(|error| panic!("live assignment: {error:?}"))
}
