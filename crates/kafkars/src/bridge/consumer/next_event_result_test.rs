//! Exhaustive assigned-event wait-error translation scenarios.

use kafka_client_engine::AssignedConsumerNextEventErrorKind;

use crate::ErrorKind;

use super::next_event_result::translate_assigned_consumer_next_event_kind;

#[test]
fn every_engine_event_wait_failure_has_a_stable_facade_category() {
    for kind in [
        AssignedConsumerNextEventErrorKind::HostUnavailable,
        AssignedConsumerNextEventErrorKind::InternalInvariant,
    ] {
        assert_eq!(
            translate_assigned_consumer_next_event_kind(kind).kind(),
            ErrorKind::Internal
        );
    }
}
