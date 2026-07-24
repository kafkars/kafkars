//! Exhaustive receive-error translation scenarios.

use kafka_client_engine::AssignedConsumerRecvErrorKind;

use crate::ErrorKind;

use super::recv_result::translate_assigned_consumer_recv_kind;

#[test]
fn every_engine_receive_failure_has_a_stable_facade_category() {
    for kind in [
        AssignedConsumerRecvErrorKind::HostUnavailable,
        AssignedConsumerRecvErrorKind::InternalInvariant,
    ] {
        assert_eq!(
            translate_assigned_consumer_recv_kind(kind).kind(),
            ErrorKind::Internal
        );
    }
}
