//! Default Error and explicit earliest/latest missing-offset ownership.

use crate::{
    AssignmentGeneration, Deadline, GroupAssignmentPartition, GroupId, GroupPositionBatch,
    GroupPositionBootstrapEffect, GroupPositionBootstrapInput, GroupPositionBootstrapMachine,
    GroupPositionBootstrapTerminal, GroupPositionFence, GroupPositionMissingOffsetPolicy,
    GroupPositionMissingOffsetReset, GroupPositionPartitionFact, MemberId, MembershipCycle, Moment,
    NextFetchOffset, PartitionIndex, StartPosition, TopicId,
};

#[test]
fn error_is_the_default_and_requests_no_reset_position() {
    assert_eq!(
        GroupPositionMissingOffsetPolicy::default(),
        GroupPositionMissingOffsetPolicy::Error
    );
    assert_eq!(
        GroupPositionMissingOffsetPolicy::Error.reset_position(),
        None
    );
}

#[test]
fn earliest_and_latest_map_to_exact_list_offsets_positions() {
    assert_eq!(
        GroupPositionMissingOffsetPolicy::Earliest.reset_position(),
        Some(StartPosition::Beginning)
    );
    assert_eq!(
        GroupPositionMissingOffsetPolicy::Latest.reset_position(),
        Some(StartPosition::End)
    );
}

#[test]
fn reset_owner_preserves_the_full_batch_and_first_missing_partition() {
    let first = assigned(1, 0);
    let second = assigned(1, 1);
    let reset = GroupPositionMissingOffsetReset::new(
        GroupPositionBatch::new(
            17,
            vec![
                GroupPositionPartitionFact::missing(first),
                GroupPositionPartitionFact::missing(second),
            ],
        ),
        0,
        StartPosition::Beginning,
    );

    assert_eq!(reset.first_missing().partition(), first);
    assert_eq!(reset.batch().facts().len(), 2);
    assert_eq!(reset.position(), StartPosition::Beginning);
    let (batch, position) = reset.into_parts();
    assert_eq!(batch.throttle_time_ms(), 17);
    assert_eq!(position, StartPosition::Beginning);
}

#[test]
fn explicit_policies_terminalize_reset_work_without_losing_committed_facts() {
    for (policy, expected_position) in [
        (
            GroupPositionMissingOffsetPolicy::Earliest,
            StartPosition::Beginning,
        ),
        (GroupPositionMissingOffsetPolicy::Latest, StartPosition::End),
    ] {
        let fence = position_fence();
        let first = assigned(1, 0);
        let missing = assigned(1, 1);
        let mut machine = GroupPositionBootstrapMachine::try_new_with_policy(
            fence,
            Deadline::from_tick(20),
            vec![first, missing],
            policy,
        )
        .unwrap_or_else(|error| panic!("policy machine: {error}"));
        machine
            .apply(GroupPositionBootstrapInput::Start {
                fence,
                now: Moment::from_tick(1),
            })
            .and_then(|_| machine.apply(GroupPositionBootstrapInput::DriverAccepted { fence }))
            .unwrap_or_else(|error| panic!("submit OffsetFetch: {error}"));
        let transition = machine
            .apply(GroupPositionBootstrapInput::OffsetsFetched {
                fence,
                now: Moment::from_tick(9),
                batch: GroupPositionBatch::new(
                    13,
                    vec![
                        GroupPositionPartitionFact::committed(
                            first,
                            NextFetchOffset::try_from_raw(7).unwrap_or_else(|| panic!("offset")),
                        ),
                        GroupPositionPartitionFact::missing(missing),
                    ],
                ),
            })
            .unwrap_or_else(|error| panic!("missing OffsetFetch result: {error}"));
        let Some(GroupPositionBootstrapEffect::Complete {
            terminal: GroupPositionBootstrapTerminal::ResetRequired(reset),
            ..
        }) = transition.into_effect()
        else {
            panic!("explicit policy must retain reset-required terminal");
        };
        assert_eq!(reset.position(), expected_position);
        assert_eq!(reset.first_missing().partition(), missing);
        assert_eq!(reset.batch().facts()[0].partition(), first);
        assert_eq!(machine.missing_offset_policy(), policy);
    }
}

fn assigned(topic: u64, partition: u32) -> GroupAssignmentPartition {
    GroupAssignmentPartition::new(
        TopicId::from_raw(topic),
        PartitionIndex::from_raw(partition),
    )
}

fn position_fence() -> GroupPositionFence {
    GroupPositionFence::new(
        GroupId::try_from_raw(1).unwrap_or_else(|| panic!("group")),
        MembershipCycle::try_from_raw(2).unwrap_or_else(|| panic!("cycle")),
        MemberId::try_from_raw(3).unwrap_or_else(|| panic!("member")),
        AssignmentGeneration::try_from_raw(4).unwrap_or_else(|| panic!("generation")),
    )
}
