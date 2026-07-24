//! Deadline ordering, cancellation, and configured-capacity scenarios.

use kafka_client_core::{AssignedConsumerEffect, AssignedConsumerInput, Deadline, Moment};

use super::assigned_timer_model::{AssignedTimerDisposition, AssignedTimerError};
use super::assigned_timers::AssignedTimers;
use super::assigned_timers_test::{arm, fetch_fence, fetch_timer, position_fence, position_timer};

#[test]
fn due_inputs_follow_deadline_then_insertion_order() {
    let (late_position, _) = position_timer(1, 3, 15);
    let (early_position, _) = position_timer(1, 4, 10);
    let (same_deadline_fetch, _) = fetch_timer(1, 5, 15);
    let late_fence = position_fence(late_position);
    let early_fence = position_fence(early_position);
    let fetch_fence = fetch_fence(same_deadline_fetch);
    let mut timers = AssignedTimers::new(3);

    assert_eq!(timers.next_deadline(), None);
    assert_eq!(
        arm(&mut timers, late_position),
        Ok(AssignedTimerDisposition::Inserted)
    );
    assert_eq!(
        arm(&mut timers, early_position),
        Ok(AssignedTimerDisposition::Inserted)
    );
    assert_eq!(
        arm(&mut timers, same_deadline_fetch),
        Ok(AssignedTimerDisposition::Inserted)
    );
    assert_eq!(timers.next_deadline(), Some(Deadline::from_tick(10)));
    assert_eq!(timers.pop_due(Moment::from_tick(9)), None);
    assert_eq!(
        timers.pop_due(Moment::from_tick(10)),
        Some(AssignedConsumerInput::PositionThrottleElapsed {
            fence: early_fence,
            now: Moment::from_tick(10),
        })
    );
    assert_eq!(timers.next_deadline(), Some(Deadline::from_tick(15)));
    assert_eq!(
        timers.pop_due(Moment::from_tick(15)),
        Some(AssignedConsumerInput::PositionThrottleElapsed {
            fence: late_fence,
            now: Moment::from_tick(15),
        })
    );
    assert_eq!(
        timers.pop_due(Moment::from_tick(15)),
        Some(AssignedConsumerInput::FetchThrottleElapsed {
            fence: fetch_fence,
            now: Moment::from_tick(15),
        })
    );
    assert_eq!(timers.timer_count(), 0);
    assert_eq!(timers.next_deadline(), None);
}

#[test]
fn revoke_cancels_only_the_owned_assignment_timer() {
    let (timer, mut machine) = fetch_timer(2, 7, 20);
    let fence = fetch_fence(timer);
    let mut timers = AssignedTimers::new(1);
    assert_eq!(
        arm(&mut timers, timer),
        Ok(AssignedTimerDisposition::Inserted)
    );
    let close = machine
        .apply(AssignedConsumerInput::BeginClose)
        .unwrap_or_else(|error| panic!("close Fetch throttle: {error}"));
    let revoke = close
        .effects()
        .iter()
        .find(|effect| {
            matches!(
                effect,
                AssignedConsumerEffect::Revoke {
                    assignment_epoch,
                    partition,
                } if *assignment_epoch == fence.position().assignment_epoch()
                    && *partition == fence.position().partition()
            )
        })
        .copied()
        .unwrap_or_else(|| panic!("close must revoke the timer assignment"));

    assert!(timers.observe_control(revoke));
    assert_eq!(timers.timer_count(), 0);
    assert_eq!(timers.next_deadline(), None);
    assert_eq!(timers.pop_due(Moment::from_tick(20)), None);
}

#[test]
fn distinct_partition_capacity_failure_returns_the_exact_effect() {
    let (first, _) = position_timer(1, 3, 10);
    let (second, _) = fetch_timer(1, 4, 10);
    let mut timers = AssignedTimers::new(1);
    assert_eq!(
        arm(&mut timers, first),
        Ok(AssignedTimerDisposition::Inserted)
    );
    assert_eq!(
        arm(&mut timers, second),
        Err(AssignedTimerError::Capacity {
            capacity: 1,
            effect: second,
        })
    );
    assert_eq!(timers.timer_count(), 1);
}
