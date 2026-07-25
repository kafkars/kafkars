//! Shared deterministic setup for installed classic-group test sessions.

use std::sync::Arc;

use kafka_client_core::{
    ClassicGeneration, ClassicGroupEffect, ClassicGroupInput, ClassicGroupTiming,
    ClassicHeartbeatPolicy, ClassicRejoinPolicy, Deadline, GroupAssignmentPartition,
    MembershipCycle, Moment,
};

use super::{classic_group_owner::ClassicGroupOwner, session_catalog::GroupSessionCatalog};

pub(super) fn timing() -> ClassicGroupTiming {
    ClassicGroupTiming::try_new(10_000, 30_000)
        .unwrap_or_else(|error| panic!("valid classic group timing: {error}"))
}

pub(super) fn heartbeat_policy() -> ClassicHeartbeatPolicy {
    ClassicHeartbeatPolicy::try_new(1_000_000_000, 2_000_000_000)
        .unwrap_or_else(|error| panic!("valid heartbeat policy: {error}"))
}

pub(super) fn rejoin_policy() -> ClassicRejoinPolicy {
    ClassicRejoinPolicy::try_new(1_000_000_000, 30_000_000_000)
        .unwrap_or_else(|error| panic!("valid rejoin policy: {error:?}"))
}

pub(super) fn begin(owner: &mut ClassicGroupOwner) -> MembershipCycle {
    owner
        .apply(ClassicGroupInput::Begin {
            now: Moment::from_tick(1),
            deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("membership begin failed: {error}"));
    owner
        .machine()
        .active_cycle()
        .unwrap_or_else(|| panic!("active membership cycle expected"))
}

pub(super) fn install_follower(
    catalog: &mut GroupSessionCatalog,
    owner: &mut ClassicGroupOwner,
    member: &str,
    generation: i32,
    partitions: Vec<GroupAssignmentPartition>,
) -> kafka_client_core::ClassicHeartbeatSchedule {
    let cycle = begin(owner);
    let candidate = catalog
        .prepare_follower_cycle(cycle, Arc::from(member))
        .unwrap_or_else(|error| panic!("follower candidate failed: {error:?}"));
    let member_id = candidate.local_member_id();
    owner
        .stage_candidate(candidate)
        .unwrap_or_else(|error| panic!("candidate stage failed: {error:?}"));
    owner
        .apply(ClassicGroupInput::JoinFollower {
            cycle,
            now: Moment::from_tick(2),
            member_id,
            generation: ClassicGeneration::try_from_raw(generation)
                .unwrap_or_else(|| panic!("nonnegative classic generation")),
        })
        .unwrap_or_else(|error| panic!("follower Join failed: {error}"));
    let transition = owner
        .apply(ClassicGroupInput::SyncSucceeded {
            cycle,
            now: Moment::from_tick(3),
            partitions,
        })
        .unwrap_or_else(|error| panic!("Sync success failed: {error}"));
    let effect = transition
        .into_effects()
        .next()
        .unwrap_or_else(|| panic!("Install effect expected"));
    let ClassicGroupEffect::Install {
        assignment,
        classic_generation,
        heartbeat,
    } = effect
    else {
        panic!("Install effect expected");
    };
    owner
        .prepare_install(catalog, assignment, classic_generation)
        .unwrap_or_else(|failure| panic!("install preparation failed: {:?}", failure.kind))
        .commit();
    heartbeat
}
