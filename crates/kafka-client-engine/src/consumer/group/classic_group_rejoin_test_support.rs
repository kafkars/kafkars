//! Shared construction of exact core and engine rejoin ownership for tests.

use kafka_client_core::{
    ClassicBrokerError, ClassicGroupEffect, ClassicGroupInput, ClassicRejoinSchedule, Deadline,
    GroupId, MembershipCycle, Moment,
};

use super::{
    classic_group_owner::ClassicGroupOwner, registry::GroupConsumerRegistry,
    registry_entry::GroupConsumerEntry,
};

pub(super) fn arm_rejoin(
    registry: &mut GroupConsumerRegistry,
    group_id: GroupId,
    rejected_at: u64,
) -> ClassicRejoinSchedule {
    let entry = entry_mut(registry, group_id);
    entry
        .classic
        .apply(ClassicGroupInput::Begin {
            now: Moment::from_tick(1),
            deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("begin failed: {error}"));
    let cycle = entry
        .classic
        .machine()
        .active_cycle()
        .unwrap_or_else(|| panic!("active cycle expected"));
    let schedule = reject_join(&mut entry.classic, cycle, rejected_at);
    entry
        .rejoin
        .prepare_rejoin_install(schedule)
        .unwrap_or_else(|error| panic!("rejoin install failed: {error:?}"))
        .commit();
    schedule
}

pub(super) fn reject_join(
    owner: &mut ClassicGroupOwner,
    cycle: MembershipCycle,
    rejected_at: u64,
) -> ClassicRejoinSchedule {
    owner
        .apply(ClassicGroupInput::JoinRejected {
            cycle,
            now: Moment::from_tick(rejected_at),
            error: ClassicBrokerError::try_from_code(14)
                .unwrap_or_else(|| panic!("nonzero broker error")),
        })
        .unwrap_or_else(|error| panic!("Join rejection failed: {error}"))
        .into_effects()
        .find_map(|effect| match effect {
            ClassicGroupEffect::ArmRejoin { schedule, .. } => Some(schedule),
            _ => None,
        })
        .unwrap_or_else(|| panic!("rejoin schedule expected"))
}

pub(super) fn entry_mut(
    registry: &mut GroupConsumerRegistry,
    group_id: GroupId,
) -> &mut GroupConsumerEntry {
    registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("registered entry expected"))
}
