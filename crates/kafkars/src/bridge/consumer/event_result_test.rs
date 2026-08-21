//! Stable facade error mapping for immediate event observation.

use kafka_client_engine::AssignedConsumerTryTakeEventErrorKind;

use super::event_result::translate_assigned_event_observation_kind;
use crate::ErrorKind;

#[test]
fn event_observation_categories_translate_exhaustively() {
    for (engine, facade) in [
        (
            AssignedConsumerTryTakeEventErrorKind::Contended,
            ErrorKind::Backpressure,
        ),
        (
            AssignedConsumerTryTakeEventErrorKind::HostUnavailable,
            ErrorKind::Internal,
        ),
        (
            AssignedConsumerTryTakeEventErrorKind::InternalInvariant,
            ErrorKind::Internal,
        ),
    ] {
        assert_eq!(
            translate_assigned_event_observation_kind(engine).kind(),
            facade
        );
    }
}
