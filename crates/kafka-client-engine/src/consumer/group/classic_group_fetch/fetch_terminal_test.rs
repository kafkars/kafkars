//! Exact broker-level classification and policy-selected offset reset transitions.

use kafka_client_core::{
    AssignedConsumerEffect, FetchFailure, GroupPositionMissingOffsetPolicy, Moment, ReadIsolation,
    StartPosition,
};

use crate::{
    clock::MonotonicClock,
    consumer::fetch_execution::{
        FetchTerminalFixture, FetchTerminalPoll, install_terminal_for_test,
    },
};

use super::{
    ClassicGroupFetchFront, ClassicGroupFetchOwner, ClassicGroupFetchOwnerFaultKind,
    model::ClassicGroupFetchOffsetResetFailure,
    test_support::{assert_attempt_deadline, catalog, committed, completed_ready, position_fence},
};

#[test]
fn partition_offset_out_of_range_uses_the_selected_reset_position() {
    for (policy, expected) in [
        (
            GroupPositionMissingOffsetPolicy::Earliest,
            StartPosition::Beginning,
        ),
        (GroupPositionMissingOffsetPolicy::Latest, StartPosition::End),
    ] {
        let (mut owner, proposal) =
            prepared_terminal(policy, FetchTerminalFixture::PartitionBroker(1));
        let clock = MonotonicClock::new();
        let before = clock
            .now()
            .unwrap_or_else(|error| panic!("clock before reset: {error}"));

        let transition = owner
            .settle_terminal_proposal(&clock, proposal)
            .unwrap_or_else(|error| panic!("settle reset proposal: {error:?}"))
            .unwrap_or_else(|| panic!("active reset transition"));

        let after = clock
            .now()
            .unwrap_or_else(|error| panic!("clock after reset: {error}"));
        let [
            AssignedConsumerEffect::Suspend { fence: suspended },
            AssignedConsumerEffect::ResolvePosition {
                fence,
                position,
                deadline,
            },
        ] = transition.effects()
        else {
            panic!(
                "reset must suspend then resolve: {:?}",
                transition.effects()
            );
        };
        assert_eq!(suspended, fence);
        assert_eq!(*position, expected);
        assert_attempt_deadline(*deadline, before, after);
        assert_eq!(owner.raw_position_deadlines.len(), 1);
        assert_eq!(owner.raw_position_deadlines[0].fence, *fence);
        assert_eq!(owner.raw_position_deadlines[0].deadline.core(), *deadline);
        assert_eq!(owner.fetches.retained(), (0, 0, 0));
        assert!(owner.fault().is_none());
    }
}

#[test]
fn admitted_recovery_capacity_failure_retains_the_exact_terminal_proposal() {
    let (mut owner, proposal) = prepared_terminal(
        GroupPositionMissingOffsetPolicy::Earliest,
        FetchTerminalFixture::PartitionBroker(1),
    );
    owner.effect_capacity = 1;

    assert!(
        owner
            .settle_terminal_proposal(&MonotonicClock::new(), proposal)
            .unwrap_or_else(|error| panic!("retain capacity failure: {error:?}"))
            .is_none()
    );

    assert_eq!(
        owner.fault().map(|fault| fault.kind()),
        Some(ClassicGroupFetchOwnerFaultKind::OffsetReset(
            ClassicGroupFetchOffsetResetFailure::EffectCapacity {
                actual: 2,
                limit: 1,
            }
        ))
    );
    assert!(owner.raw_position_deadlines.is_empty());
    let (calls, deliveries, _bytes) = owner.fetches.retained();
    assert_eq!((calls, deliveries), (1, 1));
}

#[test]
fn error_policy_and_nonmatching_broker_levels_remain_exact_fetch_failures() {
    for (policy, fixture, expected_code) in [
        (
            GroupPositionMissingOffsetPolicy::Error,
            FetchTerminalFixture::PartitionBroker(1),
            1,
        ),
        (
            GroupPositionMissingOffsetPolicy::Earliest,
            FetchTerminalFixture::Broker(1),
            1,
        ),
        (
            GroupPositionMissingOffsetPolicy::Latest,
            FetchTerminalFixture::PartitionBroker(2),
            2,
        ),
    ] {
        let (mut owner, proposal) = prepared_terminal(policy, fixture);
        let transition = owner
            .settle_terminal_proposal(&MonotonicClock::new(), proposal)
            .unwrap_or_else(|error| panic!("settle generic proposal: {error:?}"))
            .unwrap_or_else(|| panic!("generic failure transition"));

        assert!(matches!(
            transition.effects(),
            [AssignedConsumerEffect::FetchFailed {
                failure: FetchFailure::Broker(code),
                ..
            }] if code.get() == expected_code
        ));
        assert!(owner.raw_position_deadlines.is_empty());
        assert_eq!(owner.fetches.retained(), (0, 0, 0));
        assert!(owner.fault().is_none());
    }
}

fn prepared_terminal(
    policy: GroupPositionMissingOffsetPolicy,
    fixture: FetchTerminalFixture,
) -> (
    ClassicGroupFetchOwner,
    crate::consumer::fetch_execution::FetchTerminalProposal,
) {
    let catalog = catalog(&["orders"]);
    let group_fence = position_fence(7);
    let mut owner =
        ClassicGroupFetchOwner::try_new_with_policies(ReadIsolation::ReadUncommitted, policy)
            .unwrap_or_else(|error| panic!("Fetch owner: {error:?}"));
    owner
        .try_activate(
            completed_ready(
                group_fence,
                Moment::from_tick(41),
                0,
                vec![committed(1, 0, 10)],
            ),
            group_fence,
        )
        .unwrap_or_else(|error| panic!("Fetch activation: {:?}", error.kind()));
    assert_eq!(
        owner.interpret_front_effect(&catalog, &MonotonicClock::new()),
        ClassicGroupFetchFront::Interpreted
    );
    let prepared = owner
        .pop_prepared_for_test()
        .unwrap_or_else(|| panic!("prepared group Fetch"));
    install_terminal_for_test(&mut owner.fetches, prepared, fixture);
    let proposal = match owner
        .fetches
        .poll_proposal(Moment::from_tick(50))
        .unwrap_or_else(|error| panic!("poll terminal proposal: {error:?}"))
    {
        FetchTerminalPoll::Proposed(proposal) => proposal,
        FetchTerminalPoll::Idle | FetchTerminalPoll::Progressed => {
            panic!("exact terminal proposal expected")
        }
    };
    (owner, proposal)
}
