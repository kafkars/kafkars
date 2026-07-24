//! Fetch-revision exhaustion scenarios for atomic position transitions.

use crate::{Deadline, Moment, PartitionIndex, TopicId};

use super::{
    AssignedConsumerEffect, AssignedConsumerMachineError, AssignedTopicPartition, AssignmentEpoch,
    FetchRecords, FetchRevision, NextFetchOffset, PositionEpoch, PositionFence, StartPosition,
    position_state::PartitionPosition,
};

#[test]
fn fetch_revision_exhaustion_preserves_the_active_fetch_and_offset() {
    let partition = AssignedTopicPartition::new(TopicId::from_raw(1), PartitionIndex::from_raw(0));
    let position = PositionFence::new(
        AssignmentEpoch::initial(),
        partition,
        PositionEpoch::initial(),
    );
    let mut state = PartitionPosition::new(StartPosition::Offset(offset(10)));
    let first = state
        .activate(
            position,
            partition,
            Moment::from_tick(0),
            Deadline::from_tick(100),
        )
        .unwrap_or_else(|error| panic!("initial fetch activation: {error}"))
        .unwrap_or_else(|| panic!("explicit offset must activate a fetch"));
    let AssignedConsumerEffect::FetchReady {
        fence: first_fetch, ..
    } = first
    else {
        panic!("explicit offset must produce a fetch effect");
    };
    state.replace_next_fetch_revision_for_test(
        FetchRevision::try_from_raw_for_test(u64::MAX)
            .unwrap_or_else(|| panic!("maximum revision is nonzero")),
    );

    assert_eq!(
        state.advance(
            first_fetch,
            FetchRecords::NoApplicationRecords,
            offset(12),
            Moment::from_tick(1),
            5,
            partition,
        ),
        Err(AssignedConsumerMachineError::FetchRevisionExhausted { partition })
    );
    assert_eq!(
        state.advance(
            first_fetch,
            FetchRecords::NoApplicationRecords,
            offset(9),
            Moment::from_tick(1),
            0,
            partition,
        ),
        Err(AssignedConsumerMachineError::OffsetRegression {
            requested: offset(10),
            observed: offset(9),
        })
    );

    state.replace_next_fetch_revision_for_test(
        FetchRevision::try_from_raw_for_test(2)
            .unwrap_or_else(|| panic!("test revision is nonzero")),
    );
    let effects = state
        .advance(
            first_fetch,
            FetchRecords::NoApplicationRecords,
            offset(12),
            Moment::from_tick(1),
            0,
            partition,
        )
        .unwrap_or_else(|error| panic!("active fetch must survive exhaustion: {error}"));
    assert!(matches!(
        effects.as_slice(),
        [AssignedConsumerEffect::FetchReady { fence, next_offset }]
            if fence.revision().get() == 2 && *next_offset == offset(12)
    ));
}

fn offset(raw: i64) -> NextFetchOffset {
    NextFetchOffset::try_from_raw(raw).unwrap_or_else(|| panic!("nonnegative test offset"))
}
