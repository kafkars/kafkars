//! Same-attempt Join, Steady, and Leave coordinator-load retry scenarios.

use super::{
    ConsumerGroupHeartbeatAttempt, ConsumerGroupHeartbeatEffect, ConsumerGroupHeartbeatFailure,
    ConsumerGroupHeartbeatInput, ConsumerGroupHeartbeatMachine, ConsumerGroupHeartbeatPhase,
    ConsumerGroupHeartbeatPolicy, ConsumerGroupHeartbeatRequestKind,
    ConsumerGroupHeartbeatRetrySchedule,
    test_support::{deadline, member, moment, partition, succeed},
};

const ATTEMPT_TIMEOUT: u64 = 300_000_000;
const JOIN_DEADLINE: u64 = 500_000_000;

#[test]
fn join_load_retry_owns_positive_backoff_and_resubmits_the_exact_attempt() {
    let (mut machine, attempt) = joining();
    let next_sequence = machine.next_sequence;
    let schedule = arm(&mut machine, attempt, 20);

    assert_eq!(schedule.attempt(), attempt);
    assert_eq!(schedule.kind(), ConsumerGroupHeartbeatRequestKind::Join);
    assert_eq!(schedule.not_before(), deadline(100_000_020));
    assert_eq!(schedule.deadline(), deadline(JOIN_DEADLINE));
    assert_eq!(machine.retry_schedule(), Some(schedule));
    assert_eq!(machine.phase(), ConsumerGroupHeartbeatPhase::Joining);
    assert_eq!(machine.in_flight(), Some(attempt));
    assert_eq!(machine.next_sequence, next_sequence);

    let early = machine
        .apply(ConsumerGroupHeartbeatInput::CoordinatorLoadRetryDue {
            schedule,
            now: moment(schedule.not_before().tick() - 1),
        })
        .err()
        .unwrap_or_else(|| panic!("early retry must reject"));
    assert_eq!(
        early.kind(),
        super::ConsumerGroupHeartbeatErrorKind::CoordinatorLoadRetryNotDue
    );
    assert_eq!(machine.retry_schedule(), Some(schedule));

    let submit = due(&mut machine, schedule, schedule.not_before().tick());
    assert!(matches!(
        submit,
        ConsumerGroupHeartbeatEffect::Submit {
            attempt: retried,
            kind: ConsumerGroupHeartbeatRequestKind::Join,
            member_id: None,
            member_epoch: None,
            assignment_generation: None,
            deadline: original,
            ..
        } if retried == attempt && original == deadline(JOIN_DEADLINE)
    ));
    assert_eq!(machine.retry_schedule(), None);
    assert_eq!(machine.in_flight(), Some(attempt));
    assert_eq!(machine.next_sequence, next_sequence);
}

#[test]
fn steady_load_retry_retains_member_epoch_assignment_and_coordinator_semantics() {
    let (mut machine, attempt) = heartbeating();
    let next_sequence = machine.next_sequence;
    let schedule = arm(&mut machine, attempt, 26);

    assert_eq!(schedule.kind(), ConsumerGroupHeartbeatRequestKind::Steady);
    assert_eq!(schedule.not_before(), deadline(100_000_026));
    assert_eq!(schedule.deadline(), deadline(300_000_025));
    assert_eq!(
        machine
            .member_epoch()
            .map(super::identity::ConsumerGroupMemberEpoch::get),
        Some(1)
    );
    let assignment = machine
        .live_assignment()
        .unwrap_or_else(|| panic!("live assignment"));
    assert_eq!(assignment.member_id(), member(9));
    assert_eq!(assignment.assignment_generation().get(), 1);
    assert_eq!(assignment.partitions(), [partition(1, 0)]);

    let submit = due(&mut machine, schedule, schedule.not_before().tick());
    assert!(matches!(
        submit,
        ConsumerGroupHeartbeatEffect::Submit {
            attempt: retried,
            kind: ConsumerGroupHeartbeatRequestKind::Steady,
            member_id: Some(actual_member),
            member_epoch: Some(actual_epoch),
            assignment_generation: Some(generation),
            deadline: original,
            ..
        } if retried == attempt
            && actual_member == member(9)
            && actual_epoch.get() == 1
            && generation.get() == 1
            && original == deadline(300_000_025)
    ));
    assert_eq!(machine.phase(), ConsumerGroupHeartbeatPhase::Heartbeating);
    assert_eq!(machine.in_flight(), Some(attempt));
    assert_eq!(machine.next_sequence, next_sequence);
}

#[test]
fn leave_load_retry_retains_the_close_attempt_and_live_assignment() {
    let (mut machine, attempt) = leaving(500_000_000);
    let next_sequence = machine.next_sequence;
    let schedule = arm(&mut machine, attempt, 31);

    assert_eq!(schedule.kind(), ConsumerGroupHeartbeatRequestKind::Leave);
    assert_eq!(schedule.not_before(), deadline(100_000_031));
    assert_eq!(schedule.deadline(), deadline(500_000_000));
    let submit = due(&mut machine, schedule, schedule.not_before().tick());
    assert!(matches!(
        submit,
        ConsumerGroupHeartbeatEffect::Submit {
            attempt: retried,
            kind: ConsumerGroupHeartbeatRequestKind::Leave,
            member_id: Some(actual_member),
            member_epoch: Some(actual_epoch),
            assignment_generation: Some(generation),
            deadline: original,
            ..
        } if retried == attempt
            && actual_member == member(9)
            && actual_epoch.get() == 1
            && generation.get() == 1
            && original == deadline(500_000_000)
    ));
    assert_eq!(machine.phase(), ConsumerGroupHeartbeatPhase::Leaving);
    assert_eq!(machine.in_flight(), Some(attempt));
    assert_eq!(machine.next_sequence, next_sequence);
    assert!(machine.live_assignment().is_some());
}

#[test]
fn repeated_load_responses_remain_bounded_by_the_original_deadline() {
    let (mut machine, attempt) = heartbeating();
    let first = arm(&mut machine, attempt, 26);
    let _ = due(&mut machine, first, first.not_before().tick());
    let bounded = arm(&mut machine, attempt, 300_000_000);

    assert_eq!(bounded.not_before(), deadline(300_000_025));
    assert_eq!(bounded.deadline(), deadline(300_000_025));
    let terminal = machine
        .apply(ConsumerGroupHeartbeatInput::CoordinatorLoadRetryDue {
            schedule: bounded,
            now: moment(bounded.not_before().tick()),
        })
        .unwrap_or_else(|error| panic!("bounded retry deadline: {error}"))
        .into_effects()
        .collect::<Vec<_>>();
    assert!(matches!(
        terminal.as_slice(),
        [
            ConsumerGroupHeartbeatEffect::Revoke { .. },
            ConsumerGroupHeartbeatEffect::Fatal { fatal },
        ] if fatal.attempt() == attempt
            && fatal.failure() == ConsumerGroupHeartbeatFailure::DeadlineElapsed
    ));
    assert_eq!(machine.phase(), ConsumerGroupHeartbeatPhase::Fatal);
}

fn arm(
    machine: &mut ConsumerGroupHeartbeatMachine,
    attempt: ConsumerGroupHeartbeatAttempt,
    now: u64,
) -> ConsumerGroupHeartbeatRetrySchedule {
    let transition = machine
        .apply(ConsumerGroupHeartbeatInput::RetryCoordinatorLoad {
            attempt,
            now: moment(now),
            failure: ConsumerGroupHeartbeatFailure::Broker(14),
        })
        .unwrap_or_else(|error| panic!("arm coordinator-load retry: {error}"));
    let Some(ConsumerGroupHeartbeatEffect::ArmCoordinatorLoadRetry { schedule }) =
        transition.into_effects().next()
    else {
        panic!("one coordinator-load schedule")
    };
    schedule
}

fn due(
    machine: &mut ConsumerGroupHeartbeatMachine,
    schedule: ConsumerGroupHeartbeatRetrySchedule,
    now: u64,
) -> ConsumerGroupHeartbeatEffect {
    machine
        .apply(ConsumerGroupHeartbeatInput::CoordinatorLoadRetryDue {
            schedule,
            now: moment(now),
        })
        .unwrap_or_else(|error| panic!("coordinator-load retry due: {error}"))
        .into_effects()
        .next()
        .unwrap_or_else(|| panic!("retry effect"))
}

fn joining() -> (ConsumerGroupHeartbeatMachine, ConsumerGroupHeartbeatAttempt) {
    let mut machine = super::test_support::machine();
    machine.policy = ConsumerGroupHeartbeatPolicy::try_new(ATTEMPT_TIMEOUT)
        .unwrap_or_else(|_| panic!("long attempt policy"));
    let transition = machine
        .apply(ConsumerGroupHeartbeatInput::Begin {
            now: moment(10),
            deadline: deadline(JOIN_DEADLINE),
        })
        .unwrap_or_else(|error| panic!("begin Join: {error}"));
    let Some(ConsumerGroupHeartbeatEffect::Submit { attempt, .. }) =
        transition.into_effects().next()
    else {
        panic!("Join attempt")
    };
    (machine, attempt)
}

fn stable() -> ConsumerGroupHeartbeatMachine {
    let (mut machine, attempt) = joining();
    let _ = succeed(
        &mut machine,
        attempt,
        20,
        1,
        5,
        0,
        Some(vec![partition(1, 0)]),
    );
    machine
}

fn heartbeating() -> (ConsumerGroupHeartbeatMachine, ConsumerGroupHeartbeatAttempt) {
    let mut machine = stable();
    let schedule = machine.schedule().unwrap_or_else(|| panic!("cadence"));
    let transition = machine
        .apply(ConsumerGroupHeartbeatInput::HeartbeatDue {
            schedule,
            now: moment(schedule.deadline().tick()),
        })
        .unwrap_or_else(|error| panic!("steady due: {error}"));
    let Some(ConsumerGroupHeartbeatEffect::Submit { attempt, .. }) =
        transition.into_effects().next()
    else {
        panic!("steady attempt")
    };
    (machine, attempt)
}

fn leaving(deadline_tick: u64) -> (ConsumerGroupHeartbeatMachine, ConsumerGroupHeartbeatAttempt) {
    let mut machine = stable();
    let transition = machine
        .apply(ConsumerGroupHeartbeatInput::BeginLeave {
            now: moment(30),
            deadline: deadline(deadline_tick),
        })
        .unwrap_or_else(|error| panic!("begin Leave: {error}"));
    let Some(ConsumerGroupHeartbeatEffect::Submit { attempt, .. }) =
        transition.into_effects().next()
    else {
        panic!("Leave attempt")
    };
    (machine, attempt)
}
