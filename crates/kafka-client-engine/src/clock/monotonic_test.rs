//! Checked monotonic epoch and deadline-capture scenarios.

use std::time::{Duration, Instant};

use kafka_client_core::Moment;

use super::{
    ClockError, MonotonicClock,
    monotonic::{absolute_instant_after, deadline_after_moment, duration_ticks},
};

#[test]
fn one_epoch_maps_elapsed_nanoseconds_without_wall_time() {
    let epoch = Instant::now();
    let clock = MonotonicClock::from_epoch(epoch);
    let Some(boundary) = epoch.checked_add(Duration::from_nanos(17)) else {
        panic!("small monotonic addition should be representable");
    };

    assert_eq!(clock.moment_at(boundary), Ok(Moment::from_tick(17)));
}

#[test]
fn an_instant_before_the_epoch_is_rejected() {
    let epoch = Instant::now();
    let clock = MonotonicClock::from_epoch(epoch);
    let Some(before) = epoch.checked_sub(Duration::from_nanos(1)) else {
        panic!("small monotonic subtraction should be representable");
    };

    assert_eq!(clock.moment_at(before), Err(ClockError::BeforeEpoch));
}

#[test]
fn deadline_capture_uses_one_boundary_observation() {
    let epoch = Instant::now();
    let clock = MonotonicClock::from_epoch(epoch);
    let Some(boundary) = epoch.checked_add(Duration::from_nanos(23)) else {
        panic!("small monotonic addition should be representable");
    };
    let capture = clock.capture_deadline_at(boundary, Duration::from_nanos(31));

    let Ok(capture) = capture else {
        panic!("small absolute deadline should be representable");
    };
    assert_eq!(capture.now().tick(), 23);
    assert_eq!(capture.deadline().tick(), 54);
    let Some(expected_instant) = boundary.checked_add(Duration::from_nanos(31)) else {
        panic!("small monotonic addition should be representable");
    };
    assert_eq!(capture.absolute_instant(), expected_instant);
}

#[test]
fn duration_and_absolute_deadline_overflow_are_distinct() {
    assert_eq!(
        duration_ticks(Duration::new(u64::MAX, 0)),
        Err(ClockError::TickOverflow)
    );
    assert_eq!(
        deadline_after_moment(Moment::from_tick(u64::MAX), Duration::from_nanos(1)),
        Err(ClockError::DeadlineOverflow)
    );
    assert_eq!(
        absolute_instant_after(Instant::now(), Duration::new(u64::MAX, 0)),
        Err(ClockError::InstantOverflow)
    );
}

#[test]
fn public_boundary_helpers_remain_checked() {
    let clock = MonotonicClock::new();
    let capture = clock.capture_deadline_after(Duration::ZERO);
    let Ok(capture) = capture else {
        panic!("fresh zero timeout should be representable");
    };

    assert_eq!(capture.now().tick(), capture.deadline().tick());
    assert!(clock.now().is_ok());
}
