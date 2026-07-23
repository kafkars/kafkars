//! Producer public-boundary capture and linear preservation scenarios.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::{ProducerSendCapture, ProducerSendOptions};
use crate::clock::MonotonicClock;

#[test]
fn capture_retains_one_monotonic_deadline_and_boundary_timestamp() {
    let clock = MonotonicClock::new();
    let before = unix_timestamp_milliseconds();
    let capture =
        ProducerSendCapture::capture(&clock, ProducerSendOptions::new(Duration::from_secs(30)));
    let after = unix_timestamp_milliseconds();
    let Ok(capture) = capture else {
        panic!("ordinary producer boundary should be representable")
    };
    let absolute_deadline = capture.absolute_deadline();

    std::thread::sleep(Duration::from_millis(2));
    let (deadline, timestamp_ms) = capture.into_parts();

    assert_eq!(deadline.operation_deadline().transport(), absolute_deadline);
    assert!(timestamp_ms >= before);
    assert!(timestamp_ms <= after);
}

fn unix_timestamp_milliseconds() -> i64 {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO);
    i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX)
}
