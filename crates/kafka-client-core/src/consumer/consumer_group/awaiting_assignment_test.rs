//! Assignment-less KIP-848 join success, cadence, cancellation, and recovery evidence.

use super::{
    ConsumerGroupHeartbeatEffect, ConsumerGroupHeartbeatFailure, ConsumerGroupHeartbeatInput,
    ConsumerGroupHeartbeatPhase, ConsumerGroupHeartbeatRequestKind,
    test_support::{deadline, epoch, joining, member, moment, partition, succeed},
};

#[test]
fn assignment_less_join_heartbeats_until_same_or_advanced_epoch_installs_generation_one() {
    for assignment_epoch in [1, 2] {
        let (mut machine, join) = joining();
        let transition = succeed(&mut machine, join, 20, 1, 5, 0, None);
        let effects = transition.into_effects().collect::<Vec<_>>();
        let [
            ConsumerGroupHeartbeatEffect::AwaitAssignment {
                member_id,
                member_epoch,
                schedule,
            },
        ] = effects.as_slice()
        else {
            panic!("assignment-less Join must retain the accepted member")
        };
        assert_eq!(*member_id, member(9));
        assert_eq!(*member_epoch, epoch(1));
        assert_eq!(schedule.deadline(), deadline(25));
        assert_eq!(schedule.assignment_generation(), None);
        assert_eq!(
            machine.phase(),
            ConsumerGroupHeartbeatPhase::AwaitingAssignment
        );
        assert!(machine.live_assignment().is_none());

        let transition = machine
            .apply(ConsumerGroupHeartbeatInput::HeartbeatDue {
                schedule: *schedule,
                now: moment(25),
            })
            .unwrap_or_else(|error| panic!("awaiting cadence: {error}"));
        let effects = transition.into_effects().collect::<Vec<_>>();
        let [
            ConsumerGroupHeartbeatEffect::Submit {
                attempt,
                kind: ConsumerGroupHeartbeatRequestKind::Steady,
                member_epoch: Some(member_epoch),
                assignment_generation: None,
                deadline: attempt_deadline,
                ..
            },
        ] = effects.as_slice()
        else {
            panic!("awaiting cadence must send an assignment-less steady heartbeat")
        };
        assert_eq!(*member_epoch, epoch(1));
        assert_eq!(*attempt_deadline, deadline(35));

        let transition = succeed(
            &mut machine,
            *attempt,
            26,
            assignment_epoch,
            5,
            0,
            Some(vec![partition(1, 0)]),
        );
        assert!(matches!(
            transition.into_effects().next(),
            Some(ConsumerGroupHeartbeatEffect::Reconcile {
                previous: None,
                assignment,
                member_epoch,
                schedule,
            }) if assignment.assignment_generation().get() == 1
                && member_epoch == epoch(assignment_epoch)
                && schedule.assignment_generation()
                    == Some(assignment.assignment_generation())
        ));
        assert_eq!(machine.phase(), ConsumerGroupHeartbeatPhase::Stable);
    }
}

#[test]
fn repeated_missing_assignment_rearms_exact_new_epoch_and_rejects_stale_schedule() {
    let (mut machine, join) = joining();
    let first = succeed(&mut machine, join, 20, 1, 5, 0, None);
    let Some(ConsumerGroupHeartbeatEffect::AwaitAssignment {
        schedule: first_schedule,
        ..
    }) = first.into_effects().next()
    else {
        panic!("first awaiting schedule")
    };
    let steady = machine
        .apply(ConsumerGroupHeartbeatInput::HeartbeatDue {
            schedule: first_schedule,
            now: moment(25),
        })
        .unwrap_or_else(|error| panic!("first due: {error}"));
    let Some(ConsumerGroupHeartbeatEffect::Submit { attempt, .. }) = steady.into_effects().next()
    else {
        panic!("first steady attempt")
    };
    let repeated = succeed(&mut machine, attempt, 26, 2, 7, 0, None);
    let Some(ConsumerGroupHeartbeatEffect::AwaitAssignment {
        schedule: next_schedule,
        ..
    }) = repeated.into_effects().next()
    else {
        panic!("repeated awaiting schedule")
    };
    assert_eq!(next_schedule.attempt().member_epoch(), Some(epoch(2)));
    assert_eq!(next_schedule.deadline(), deadline(33));
    assert_eq!(next_schedule.assignment_generation(), None);

    let error = machine
        .apply(ConsumerGroupHeartbeatInput::HeartbeatDue {
            schedule: first_schedule,
            now: moment(33),
        })
        .err()
        .unwrap_or_else(|| panic!("stale schedule must reject"));
    assert_eq!(
        error.kind(),
        super::ConsumerGroupHeartbeatErrorKind::ScheduleMismatch
    );
    assert_eq!(machine.schedule(), Some(next_schedule));
    assert_eq!(machine.member_epoch(), Some(epoch(2)));
}

#[test]
fn assignment_less_member_leaves_without_inventing_an_assignment_fence() {
    let (mut machine, join) = joining();
    let _ = succeed(&mut machine, join, 20, 1, 5, 0, None);
    let transition = machine
        .apply(ConsumerGroupHeartbeatInput::BeginLeave {
            now: moment(22),
            deadline: deadline(40),
        })
        .unwrap_or_else(|error| panic!("pending leave: {error}"));
    let Some(ConsumerGroupHeartbeatEffect::Submit {
        attempt: leave,
        kind: ConsumerGroupHeartbeatRequestKind::Leave,
        assignment_generation: None,
        ..
    }) = transition.into_effects().next()
    else {
        panic!("pending member must emit an assignment-less Leave")
    };
    let closed = machine
        .apply(ConsumerGroupHeartbeatInput::LeaveSucceeded { attempt: leave })
        .unwrap_or_else(|error| panic!("leave success: {error}"));
    assert_eq!(closed.effects().count(), 0);
    assert_eq!(machine.phase(), ConsumerGroupHeartbeatPhase::Closed);
}

#[test]
fn assignment_less_steady_rediscovery_needs_no_assignment_fence() {
    let (mut machine, join) = joining();
    let awaiting = succeed(&mut machine, join, 20, 1, 5, 0, None);
    let Some(ConsumerGroupHeartbeatEffect::AwaitAssignment { schedule, .. }) =
        awaiting.into_effects().next()
    else {
        panic!("awaiting schedule")
    };
    let due = machine
        .apply(ConsumerGroupHeartbeatInput::HeartbeatDue {
            schedule,
            now: moment(25),
        })
        .unwrap_or_else(|error| panic!("due heartbeat: {error}"));
    let Some(ConsumerGroupHeartbeatEffect::Submit { attempt, .. }) = due.into_effects().next()
    else {
        panic!("steady attempt")
    };
    let retry = machine
        .apply(ConsumerGroupHeartbeatInput::RetryHeartbeat {
            attempt,
            now: moment(26),
            failure: ConsumerGroupHeartbeatFailure::CoordinatorUnavailable,
        })
        .unwrap_or_else(|error| panic!("rediscovery: {error}"));
    assert!(matches!(
        retry.into_effects().next(),
        Some(ConsumerGroupHeartbeatEffect::Rediscover {
            attempt: replacement,
            assignment_generation: None,
            deadline: original,
            ..
        }) if replacement != attempt && original == deadline(35)
    ));
}
