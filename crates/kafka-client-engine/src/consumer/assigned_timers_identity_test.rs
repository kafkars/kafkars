//! Equal-fence identity and deadline-conflict scenarios.

use kafka_client_core::{AssignedConsumerEffect, AssignedConsumerInput, Deadline, Moment};

use super::assigned_timer_model::{AssignedTimerDisposition, AssignedTimerError};
use super::assigned_timers::AssignedTimers;
use super::assigned_timers_test::{arm, position_fence, position_timer};

#[test]
fn equal_fence_and_deadline_is_idempotent_without_reordering() {
    let (first, _) = position_timer(1, 3, 10);
    let (second, _) = position_timer(1, 4, 10);
    let first_fence = position_fence(first);
    let second_fence = position_fence(second);
    let mut timers = AssignedTimers::new(2);

    assert_eq!(
        arm(&mut timers, first),
        Ok(AssignedTimerDisposition::Inserted)
    );
    assert_eq!(
        arm(&mut timers, second),
        Ok(AssignedTimerDisposition::Inserted)
    );
    assert_eq!(
        arm(&mut timers, first),
        Ok(AssignedTimerDisposition::Idempotent)
    );
    assert_eq!(timers.timer_count(), 2);
    assert_eq!(
        timers.pop_due(Moment::from_tick(10)),
        Some(AssignedConsumerInput::PositionThrottleElapsed {
            fence: first_fence,
            now: Moment::from_tick(10),
        })
    );
    assert_eq!(
        timers.pop_due(Moment::from_tick(10)),
        Some(AssignedConsumerInput::PositionThrottleElapsed {
            fence: second_fence,
            now: Moment::from_tick(10),
        })
    );
}

#[test]
fn equal_fence_with_different_deadline_is_lossless_conflict() {
    let (active, _) = position_timer(1, 3, 15);
    let fence = position_fence(active);
    let incoming = AssignedConsumerEffect::ArmPositionThrottle {
        fence,
        deadline: Deadline::from_tick(20),
    };
    let mut timers = AssignedTimers::new(1);
    assert_eq!(
        arm(&mut timers, active),
        Ok(AssignedTimerDisposition::Inserted)
    );

    assert_eq!(
        arm(&mut timers, incoming),
        Err(AssignedTimerError::DeadlineConflict {
            active_deadline: Deadline::from_tick(15),
            effect: incoming,
        })
    );
    assert_eq!(timers.timer_count(), 1);
    assert_eq!(timers.next_deadline(), Some(Deadline::from_tick(15)));
    assert_eq!(timers.pop_due(Moment::from_tick(14)), None);
    assert_eq!(
        timers.pop_due(Moment::from_tick(15)),
        Some(AssignedConsumerInput::PositionThrottleElapsed {
            fence,
            now: Moment::from_tick(15),
        })
    );
}
