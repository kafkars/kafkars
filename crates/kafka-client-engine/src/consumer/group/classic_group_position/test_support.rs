//! Shared completed-position fixtures for group consumer scenario tests.

use kafka_client_core::{
    Deadline, GroupPositionBatch, GroupPositionBootstrapEffect, GroupPositionBootstrapInput,
    GroupPositionBootstrapMachine, GroupPositionFence, Moment,
};

use super::ClassicGroupPositionCompleted;

pub(in crate::consumer::group) fn completed_ready(
    fence: GroupPositionFence,
    observed_at: Moment,
    batch: GroupPositionBatch,
) -> ClassicGroupPositionCompleted {
    let partitions = batch.facts().iter().map(|fact| fact.partition()).collect();
    let mut machine =
        GroupPositionBootstrapMachine::try_new(fence, Deadline::from_tick(u64::MAX), partitions)
            .unwrap_or_else(|error| panic!("position machine: {error}"));
    let start = machine
        .apply(GroupPositionBootstrapInput::Start {
            fence,
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("position start: {error}"));
    assert!(matches!(
        start.into_effect(),
        Some(GroupPositionBootstrapEffect::FetchOffsets { .. })
    ));
    machine
        .apply(GroupPositionBootstrapInput::DriverAccepted { fence })
        .unwrap_or_else(|error| panic!("position acceptance: {error}"));
    let transition = machine
        .apply(GroupPositionBootstrapInput::OffsetsFetched {
            fence,
            now: observed_at,
            batch,
        })
        .unwrap_or_else(|error| panic!("position terminal: {error}"));
    let Some(GroupPositionBootstrapEffect::Complete { terminal, .. }) = transition.into_effect()
    else {
        panic!("position completion expected");
    };
    ClassicGroupPositionCompleted::new(machine, terminal, observed_at)
}
