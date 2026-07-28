//! Exact scalar Join retention and directional cycle-fault evidence.

use kafka_client_core::{ClassicGroupEffect, ClassicProtocol, Deadline, GroupId, MembershipCycle};

use super::{
    classic_group_entry_fault::ClassicGroupEntryFault,
    classic_group_rejoin_fault::{ClassicRejoinPostCore, ClassicRejoinPostCoreFailure},
    classic_group_rejoin_test_support::entry_mut,
    registry_test_support::{register, started_registry},
};

#[test]
fn post_core_fault_retains_the_exact_scalar_join_owner() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    let cycle = MembershipCycle::try_from_raw(3).unwrap_or_else(|| panic!("nonzero skipped cycle"));
    let core_deadline = Deadline::from_tick(47);
    let timing = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("registered entry expected"))
        .classic
        .machine()
        .timing();
    entry_mut(&mut registry, group_id).fault = Some(ClassicGroupEntryFault::RejoinPostCore(
        ClassicRejoinPostCore::join_for_test(
            group_id,
            cycle,
            timing,
            core_deadline,
            ClassicRejoinPostCoreFailure::CycleSequence,
        ),
    ));

    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("registered entry expected"));
    let Some(ClassicGroupEntryFault::RejoinPostCore(fault)) = &entry.fault else {
        panic!("post-core rejoin fault expected");
    };
    let join = fault
        .join()
        .unwrap_or_else(|| panic!("exact scalar Join expected"));
    assert_eq!(join.cycle(), cycle);
    assert_eq!(join.deadline(), core_deadline);
    assert_eq!(fault.failure(), ClassicRejoinPostCoreFailure::CycleSequence);
    assert_eq!(registry.membership_unsettled(), 1);
}

#[test]
fn impossible_two_effect_shape_is_retained_without_dropping_either_slot() {
    let group_id = GroupId::try_from_raw(41).unwrap_or_else(|| panic!("nonzero group identity"));
    let timing = super::classic_group_test_support::timing();
    let first_cycle = MembershipCycle::initial();
    let second_cycle = first_cycle
        .checked_next()
        .unwrap_or_else(|| panic!("next cycle expected"));
    let first = join_effect(group_id, first_cycle, timing, Deadline::from_tick(10));
    let second = join_effect(group_id, second_cycle, timing, Deadline::from_tick(20));
    let fault = ClassicRejoinPostCore::new(
        None,
        [Some(first), Some(second)],
        ClassicRejoinPostCoreFailure::EffectShape,
    );

    assert_eq!(fault.retained_owner_count(), 2);
    assert!(matches!(
        &fault.other()[0],
        Some(ClassicGroupEffect::Join { cycle, .. }) if *cycle == first_cycle
    ));
    assert!(matches!(
        &fault.other()[1],
        Some(ClassicGroupEffect::Join { cycle, .. }) if *cycle == second_cycle
    ));
}

fn join_effect(
    group_id: GroupId,
    cycle: MembershipCycle,
    timing: kafka_client_core::ClassicGroupTiming,
    deadline: Deadline,
) -> ClassicGroupEffect {
    ClassicGroupEffect::Join {
        group_id,
        cycle,
        protocol: ClassicProtocol::Range,
        member_id: None,
        timing,
        deadline,
    }
}
