//! Retained-member coordinator-load retry for a fenced-recovery Join.

use super::{
    ConsumerGroupHeartbeatAttempt, ConsumerGroupHeartbeatEffect, ConsumerGroupHeartbeatFailure,
    ConsumerGroupHeartbeatInput, ConsumerGroupHeartbeatMachine, ConsumerGroupHeartbeatPolicy,
    ConsumerGroupHeartbeatRequestKind,
    test_support::{deadline, member, moment, partition, succeed},
};

#[test]
fn fenced_recovery_join_load_retry_retains_the_member_and_recovery_deadline() {
    let (mut machine, steady_attempt) = long_heartbeating();
    let recovery = machine
        .apply(ConsumerGroupHeartbeatInput::RecoverFencedMembership {
            attempt: steady_attempt,
            now: moment(26),
            failure: ConsumerGroupHeartbeatFailure::Broker(110),
        })
        .unwrap_or_else(|error| panic!("fenced recovery: {error}"));
    let Some(ConsumerGroupHeartbeatEffect::Submit {
        attempt,
        kind: ConsumerGroupHeartbeatRequestKind::Join,
        member_id: Some(recovery_member),
        deadline: recovery_deadline,
        ..
    }) = recovery.into_effects().nth(1)
    else {
        panic!("retained-member recovery Join")
    };
    assert_eq!(recovery_member, member(9));
    assert_eq!(recovery_deadline, deadline(300_000_026));

    let retry = machine
        .apply(ConsumerGroupHeartbeatInput::RetryCoordinatorLoad {
            attempt,
            now: moment(27),
            failure: ConsumerGroupHeartbeatFailure::Broker(14),
        })
        .unwrap_or_else(|error| panic!("arm recovery Join retry: {error}"));
    let Some(ConsumerGroupHeartbeatEffect::ArmCoordinatorLoadRetry { schedule }) =
        retry.into_effects().next()
    else {
        panic!("recovery Join retry schedule")
    };
    assert_eq!(schedule.not_before(), deadline(100_000_027));
    assert_eq!(schedule.deadline(), recovery_deadline);

    let due = machine
        .apply(ConsumerGroupHeartbeatInput::CoordinatorLoadRetryDue {
            schedule,
            now: moment(schedule.not_before().tick()),
        })
        .unwrap_or_else(|error| panic!("recovery Join retry due: {error}"));
    assert!(matches!(
        due.into_effects().next(),
        Some(ConsumerGroupHeartbeatEffect::Submit {
            attempt: retried,
            kind: ConsumerGroupHeartbeatRequestKind::Join,
            member_id: Some(actual_member),
            member_epoch: None,
            assignment_generation: None,
            deadline: original,
            ..
        }) if retried == attempt
            && actual_member == member(9)
            && original == recovery_deadline
    ));
}

fn long_heartbeating() -> (ConsumerGroupHeartbeatMachine, ConsumerGroupHeartbeatAttempt) {
    let mut machine = super::test_support::machine();
    machine.policy = ConsumerGroupHeartbeatPolicy::try_new(300_000_000)
        .unwrap_or_else(|_| panic!("long attempt policy"));
    let begin = machine
        .apply(ConsumerGroupHeartbeatInput::Begin {
            now: moment(10),
            deadline: deadline(500_000_000),
        })
        .unwrap_or_else(|error| panic!("begin Join: {error}"));
    let Some(ConsumerGroupHeartbeatEffect::Submit {
        attempt: join_attempt,
        ..
    }) = begin.into_effects().next()
    else {
        panic!("Join attempt")
    };
    let _ = succeed(
        &mut machine,
        join_attempt,
        20,
        1,
        5,
        0,
        Some(vec![partition(1, 0)]),
    );
    let schedule = machine.schedule().unwrap_or_else(|| panic!("cadence"));
    let heartbeat = machine
        .apply(ConsumerGroupHeartbeatInput::HeartbeatDue {
            schedule,
            now: moment(schedule.deadline().tick()),
        })
        .unwrap_or_else(|error| panic!("steady due: {error}"));
    let Some(ConsumerGroupHeartbeatEffect::Submit { attempt, .. }) =
        heartbeat.into_effects().next()
    else {
        panic!("steady attempt")
    };
    (machine, attempt)
}
