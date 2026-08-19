//! Cooperative retain-on-rebalance and eager-loss regression tests.

#![expect(
    clippy::too_many_lines,
    reason = "the scenario intentionally keeps one rebalance lifecycle visible"
)]

use crate::{
    AssignmentGeneration, Deadline, GroupAssignmentPartition, GroupId, MemberId, Moment,
    PartitionIndex, TopicId,
};

use super::{
    ClassicBrokerError, ClassicGeneration, ClassicGroupEffect, ClassicGroupInput,
    ClassicGroupMachine, ClassicGroupPhase, ClassicGroupTiming, ClassicHeartbeatAttempt,
    ClassicHeartbeatPolicy, ClassicProtocol, ClassicRejoinPolicy,
};

#[test]
fn cooperative_rebalance_retains_live_ownership_through_scheduled_rejoin() {
    let (mut machine, attempt) = stable(ClassicProtocol::CooperativeSticky);

    let transition = reject(&mut machine, attempt, 27);

    assert_eq!(transition.effects().count(), 1);
    let Some(ClassicGroupEffect::ArmRejoin { schedule, .. }) = transition.effects().next() else {
        panic!("retained ArmRejoin expected");
    };
    assert!(machine.live_assignment().is_some());
    assert_eq!(machine.live_cycle(), Some(attempt.cycle()));
    assert_eq!(machine.phase(), ClassicGroupPhase::WaitingToRejoin);
    let schedule = *schedule;
    let join = machine
        .apply(ClassicGroupInput::RejoinDue {
            schedule,
            now: Moment::from_tick(schedule.due().tick()),
        })
        .unwrap_or_else(|error| panic!("retained rejoin: {error}"));
    assert!(matches!(
        join.effects().next(),
        Some(ClassicGroupEffect::Join {
            member_id: Some(member_id),
            ..
        }) if member_id.get() == 1
    ));
    assert!(machine.live_assignment().is_some());
}

#[test]
fn range_rebalance_still_eagerly_revokes_before_rejoin() {
    let (mut machine, attempt) = stable(ClassicProtocol::Range);

    let transition = reject(&mut machine, attempt, 27);

    let effects: Vec<_> = transition.effects().collect();
    assert!(matches!(
        effects.first(),
        Some(ClassicGroupEffect::Revoke { .. })
    ));
    assert!(matches!(
        effects.get(1),
        Some(ClassicGroupEffect::ArmRejoin { .. })
    ));
    assert!(machine.live_assignment().is_none());
    assert_eq!(machine.live_cycle(), None);
}

#[test]
fn unknown_member_eagerly_revokes_cooperative_ownership() {
    let (mut machine, attempt) = stable(ClassicProtocol::CooperativeSticky);

    let transition = reject(&mut machine, attempt, 25);

    let effects: Vec<_> = transition.effects().collect();
    assert!(matches!(
        effects.first(),
        Some(ClassicGroupEffect::Revoke { .. })
    ));
    assert!(matches!(
        effects.get(1),
        Some(ClassicGroupEffect::ArmRejoin { .. })
    ));
    assert!(machine.live_assignment().is_none());
    assert_eq!(machine.live_generation(), None);
    assert_eq!(machine.live_cycle(), None);
}

#[test]
fn rebalance_during_reconciliation_defers_rejoin_until_replacement_is_applied() {
    let (mut machine, initial_attempt) =
        stable_with_partitions(ClassicProtocol::CooperativeSticky, &[0, 1]);
    let initial_rejection = reject(&mut machine, initial_attempt, 27);
    let Some(ClassicGroupEffect::ArmRejoin {
        schedule: initial_rejoin,
        ..
    }) = initial_rejection.effects().next()
    else {
        panic!("initial cooperative rejoin schedule");
    };
    let join = machine
        .apply(ClassicGroupInput::RejoinDue {
            schedule: *initial_rejoin,
            now: Moment::from_tick(initial_rejoin.due().tick()),
        })
        .unwrap_or_else(|error| panic!("initial retained rejoin: {error}"));
    let Some(ClassicGroupEffect::Join { cycle, .. }) = join.effects().next() else {
        panic!("replacement cooperative Join");
    };
    let cycle = *cycle;
    machine
        .apply(ClassicGroupInput::JoinFollower {
            cycle,
            now: Moment::from_tick(10),
            member_id: MemberId::try_from_raw(1).unwrap_or_else(|| panic!("member")),
            generation: ClassicGeneration::try_from_raw(8).unwrap_or_else(|| panic!("generation")),
        })
        .unwrap_or_else(|error| panic!("replacement Join: {error}"));
    let sync = machine
        .apply(ClassicGroupInput::SyncSucceeded {
            cycle,
            now: Moment::from_tick(11),
            partitions: partitions(&[0, 2]),
        })
        .unwrap_or_else(|error| panic!("replacement Sync: {error}"));
    let Some(ClassicGroupEffect::Reconcile { reconciliation }) = sync.effects().next() else {
        panic!("replacement reconciliation");
    };
    let heartbeat = reconciliation.heartbeat();
    let assignment_generation = reconciliation
        .replacement_assignment()
        .assignment_generation();
    machine
        .apply(ClassicGroupInput::HeartbeatDue {
            attempt: heartbeat.attempt(),
            now: Moment::from_tick(heartbeat.due().tick()),
        })
        .unwrap_or_else(|error| panic!("reconciliation Heartbeat due: {error}"));
    let rejection = machine
        .apply(ClassicGroupInput::HeartbeatRejected {
            attempt: heartbeat.attempt(),
            now: Moment::from_tick(heartbeat.due().tick()),
            error: ClassicBrokerError::try_from_code(27)
                .unwrap_or_else(|| panic!("rebalance-in-progress")),
        })
        .unwrap_or_else(|error| panic!("reconciliation Heartbeat rejection: {error}"));
    let Some(ClassicGroupEffect::ArmRejoin { schedule, .. }) = rejection.effects().next() else {
        panic!("deferred cooperative rejoin schedule");
    };
    let schedule = *schedule;
    assert_eq!(machine.phase(), ClassicGroupPhase::WaitingToRejoin);
    assert!(machine.pending_reconciliation.is_some());

    let early = machine
        .apply(ClassicGroupInput::RejoinDue {
            schedule,
            now: Moment::from_tick(schedule.due().tick()),
        })
        .err()
        .unwrap_or_else(|| panic!("rejoin must remain fenced by reconciliation"));
    assert_eq!(early.kind(), super::ClassicGroupErrorKind::InvalidPhase);
    assert!(machine.pending_reconciliation.is_some());

    let applied = machine
        .apply(ClassicGroupInput::ReconciliationApplied {
            cycle,
            assignment_generation,
            now: Moment::from_tick(schedule.due().tick()),
        })
        .unwrap_or_else(|error| panic!("apply replacement before rejoin: {error}"));
    assert_eq!(applied.effects().count(), 0);
    assert_eq!(machine.phase(), ClassicGroupPhase::WaitingToRejoin);
    assert_eq!(machine.pending_rejoin(), Some(schedule));
    assert!(machine.pending_reconciliation.is_none());
    assert_eq!(
        machine
            .live_assignment()
            .unwrap_or_else(|| panic!("replacement ownership"))
            .assignment_generation(),
        AssignmentGeneration::try_from_raw(2).unwrap_or_else(|| panic!("assignment generation"))
    );
    assert!(matches!(
        machine
            .apply(ClassicGroupInput::RejoinDue {
                schedule,
                now: Moment::from_tick(schedule.due().tick()),
            })
            .unwrap_or_else(|error| panic!("resume deferred rejoin: {error}"))
            .effects()
            .next(),
        Some(ClassicGroupEffect::Join {
            member_id: Some(_),
            ..
        })
    ));
}

fn reject(
    machine: &mut ClassicGroupMachine,
    attempt: ClassicHeartbeatAttempt,
    code: i16,
) -> super::ClassicGroupTransition {
    machine
        .apply(ClassicGroupInput::HeartbeatRejected {
            attempt,
            now: Moment::from_tick(4),
            error: ClassicBrokerError::try_from_code(code)
                .unwrap_or_else(|| panic!("nonzero broker error")),
        })
        .unwrap_or_else(|error| panic!("heartbeat rejection: {error}"))
}

fn stable(protocol: ClassicProtocol) -> (ClassicGroupMachine, ClassicHeartbeatAttempt) {
    stable_with_partitions(protocol, &[])
}

fn stable_with_partitions(
    protocol: ClassicProtocol,
    assigned: &[u32],
) -> (ClassicGroupMachine, ClassicHeartbeatAttempt) {
    let mut machine = ClassicGroupMachine::new_with_protocol(
        GroupId::try_from_raw(1).unwrap_or_else(|| panic!("group")),
        protocol,
        ClassicGroupTiming::try_new(10_000, 30_000)
            .unwrap_or_else(|error| panic!("timing: {error}")),
        ClassicHeartbeatPolicy::try_new(10, 20)
            .unwrap_or_else(|error| panic!("heartbeat: {error}")),
        ClassicRejoinPolicy::try_new(5, 50).unwrap_or_else(|error| panic!("rejoin: {error:?}")),
    );
    machine
        .apply(ClassicGroupInput::Begin {
            now: Moment::from_tick(1),
            deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("Begin: {error}"));
    let cycle = machine.active_cycle().unwrap_or_else(|| panic!("cycle"));
    machine
        .apply(ClassicGroupInput::JoinFollower {
            cycle,
            now: Moment::from_tick(2),
            member_id: MemberId::try_from_raw(1).unwrap_or_else(|| panic!("member")),
            generation: ClassicGeneration::try_from_raw(7).unwrap_or_else(|| panic!("generation")),
        })
        .unwrap_or_else(|error| panic!("Join: {error}"));
    let install = machine
        .apply(ClassicGroupInput::SyncSucceeded {
            cycle,
            now: Moment::from_tick(3),
            partitions: partitions(assigned),
        })
        .unwrap_or_else(|error| panic!("Sync: {error}"));
    let Some(ClassicGroupEffect::Install { heartbeat, .. }) = install.effects().next() else {
        panic!("Install expected");
    };
    let attempt = heartbeat.attempt();
    machine
        .apply(ClassicGroupInput::HeartbeatDue {
            attempt,
            now: Moment::from_tick(heartbeat.due().tick()),
        })
        .unwrap_or_else(|error| panic!("heartbeat due: {error}"));
    (machine, attempt)
}

fn partitions(values: &[u32]) -> Vec<GroupAssignmentPartition> {
    values
        .iter()
        .copied()
        .map(|partition| {
            GroupAssignmentPartition::new(TopicId::from_raw(1), PartitionIndex::from_raw(partition))
        })
        .collect()
}
