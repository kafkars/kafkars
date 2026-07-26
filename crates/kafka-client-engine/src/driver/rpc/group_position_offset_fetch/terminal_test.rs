//! Raw selected-version, response, and exact-key retention scenarios.

use kafka_client_core::Deadline;
use kafka_driver::{ApiKey, ApiVersion, CallFailure, Delivery, RequestError};
use kafka_wire_core::{DecodeError, EncodeError};

use super::{
    calls::TrackedGroupPositionOffsetFetchCalls,
    calls_test::{fence, key},
    terminal::{GroupPositionOffsetFetchDriverFailureKind, GroupPositionOffsetFetchTerminalFact},
};

#[test]
fn raw_terminal_preserves_key_selected_version_and_uninterpreted_response() {
    let mut calls = TrackedGroupPositionOffsetFetchCalls::new(8);
    let mut response = kafka_wire::OffsetFetchResponse::default();
    response.throttle_time_ms = 17;
    calls.install_terminal_for_test(key(5, 151), Some(8), Ok(response));
    let accepted =
        super::admission::GroupPositionOffsetFetchAccepted::from_fence_for_test(fence(5));
    let terminal = calls
        .begin_group_position_offset_fetch_settlement(&accepted)
        .unwrap_or_else(|error| panic!("begin terminal: {error:?}"));

    assert_eq!(terminal.key().fence(), fence(5));
    assert_eq!(
        terminal.key().operation_deadline().core(),
        Deadline::from_tick(151)
    );
    let GroupPositionOffsetFetchTerminalFact::Response {
        selected_version,
        response,
    } = terminal.fact()
    else {
        panic!("raw response expected");
    };
    assert_eq!(selected_version, Some(8));
    assert_eq!(response.throttle_time_ms, 17);
}

#[test]
fn raw_failures_map_to_every_closed_semantic_category() {
    let cases = [
        (
            RequestError::Rejected {
                failure: CallFailure::DeadlineExceeded,
                delivery: Delivery::PossiblySent,
            },
            GroupPositionOffsetFetchDriverFailureKind::DeadlineElapsed,
        ),
        (
            RequestError::VersionFloorUnavailable {
                api_key: ApiKey::new(9),
                minimum: ApiVersion::new(7),
                negotiated_maximum: ApiVersion::new(6),
            },
            GroupPositionOffsetFetchDriverFailureKind::Compatibility,
        ),
        (
            RequestError::Decode(DecodeError::UnexpectedEnd {
                offset: 1,
                needed: 4,
                remaining: 0,
            }),
            GroupPositionOffsetFetchDriverFailureKind::InvalidResponse,
        ),
        (
            RequestError::RouteUnavailable,
            GroupPositionOffsetFetchDriverFailureKind::Transport,
        ),
    ];
    for (index, (error, expected)) in cases.into_iter().enumerate() {
        let terminal = terminal_with_error(index as u64 + 20, error);
        let GroupPositionOffsetFetchTerminalFact::Failed { kind } = terminal.fact() else {
            panic!("failure fact expected");
        };
        assert_eq!(kind, expected);
    }
}

#[test]
fn every_local_or_negotiated_version_failure_is_compatibility() {
    let failures = [
        RequestError::Encode(EncodeError::LengthOverflow {
            kind: "group id",
            length: usize::MAX,
            maximum: i16::MAX as usize,
        }),
        RequestError::UnsupportedVersion {
            message: "OffsetFetch request",
            version: ApiVersion::new(10),
        },
        RequestError::ApiUnavailable {
            api_key: ApiKey::new(9),
        },
        RequestError::VersionLimitUnavailable {
            api_key: ApiKey::new(9),
            maximum: ApiVersion::new(9),
            negotiated_minimum: ApiVersion::new(10),
        },
        RequestError::VersionFloorUnavailable {
            api_key: ApiKey::new(9),
            minimum: ApiVersion::new(6),
            negotiated_maximum: ApiVersion::new(5),
        },
        RequestError::VersionBoundsInvalid {
            api_key: ApiKey::new(9),
            minimum: ApiVersion::new(6),
            maximum: ApiVersion::new(5),
        },
    ];
    for (index, failure) in failures.into_iter().enumerate() {
        let terminal = terminal_with_error(index as u64 + 30, failure);
        let GroupPositionOffsetFetchTerminalFact::Failed { kind } = terminal.fact() else {
            panic!("failure fact expected");
        };
        assert_eq!(
            kind,
            GroupPositionOffsetFetchDriverFailureKind::Compatibility
        );
    }
}

fn terminal_with_error(
    fence_raw: u64,
    error: RequestError,
) -> super::terminal::GroupPositionOffsetFetchTerminal {
    let mut calls = TrackedGroupPositionOffsetFetchCalls::new(1);
    let key = key(fence_raw, 200 + fence_raw);
    let accepted =
        super::admission::GroupPositionOffsetFetchAccepted::from_fence_for_test(key.fence());
    calls.install_terminal_for_test(key, None, Err(error));
    calls
        .begin_group_position_offset_fetch_settlement(&accepted)
        .unwrap_or_else(|failure| panic!("terminal settlement: {failure:?}"))
}
