//! Classic Heartbeat response version, throttle, and broker-error scenarios.

use kafka_wire::HeartbeatResponse;

use super::{
    ClassicHeartbeatOutcome, ClassicHeartbeatResponseFailure, normalize_classic_heartbeat_response,
};

#[test]
fn success_preserves_nonnegative_throttle_without_generated_types() {
    for (version, throttle_time_ms) in [(0, 0), (1, 7), (2, i32::MAX)] {
        let mut response = HeartbeatResponse::default();
        response.throttle_time_ms = throttle_time_ms;
        assert_eq!(
            normalize_classic_heartbeat_response(version, &response),
            Ok(ClassicHeartbeatOutcome::Succeeded {
                throttle_time_ms: u32::try_from(throttle_time_ms)
                    .unwrap_or_else(|_| panic!("nonnegative throttle")),
            })
        );
    }
}

#[test]
fn exact_v0_v2_window_and_throttle_shape_are_enforced() {
    let response = HeartbeatResponse::default();
    for version in [-1, 4] {
        assert_eq!(
            normalize_classic_heartbeat_response(version, &response),
            Err(ClassicHeartbeatResponseFailure::UnsupportedApiVersion(
                version
            ))
        );
    }

    let mut impossible_v0 = HeartbeatResponse::default();
    impossible_v0.throttle_time_ms = 1;
    assert_eq!(
        normalize_classic_heartbeat_response(0, &impossible_v0),
        Err(ClassicHeartbeatResponseFailure::UnexpectedThrottleTime(1))
    );

    let mut negative = HeartbeatResponse::default();
    negative.throttle_time_ms = -1;
    assert_eq!(
        normalize_classic_heartbeat_response(2, &negative),
        Err(ClassicHeartbeatResponseFailure::NegativeThrottleTime(-1))
    );
}

#[test]
fn exact_nonzero_signed_broker_code_is_preserved_without_classification() {
    for error_code in [i16::MIN, -321, 321, i16::MAX] {
        let mut response = HeartbeatResponse::default();
        response.throttle_time_ms = 8;
        response.error_code = error_code;
        let normalized = normalize_classic_heartbeat_response(2, &response)
            .unwrap_or_else(|error| panic!("broker rejection failed: {error:?}"));
        let ClassicHeartbeatOutcome::Rejected(rejection) = normalized else {
            panic!("broker rejection expected");
        };
        assert_eq!(rejection.error_code().get(), error_code);
        assert_eq!(rejection.throttle_time_ms(), 8);
    }
}

#[test]
fn v0_broker_rejection_cannot_smuggle_an_unrepresentable_throttle() {
    let mut response = HeartbeatResponse::default();
    response.throttle_time_ms = 1;
    response.error_code = -321;
    assert_eq!(
        normalize_classic_heartbeat_response(0, &response),
        Err(ClassicHeartbeatResponseFailure::UnexpectedThrottleTime(1))
    );
}
