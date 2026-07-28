//! Hosted group receive failure translation contract.

use kafka_client_engine::GroupConsumerRecvErrorKind;

use crate::ErrorKind;

use super::group_consumer_recv_result::translate_group_consumer_recv_kind;

#[test]
fn host_failures_remain_internal() {
    for kind in [
        GroupConsumerRecvErrorKind::HostUnavailable,
        GroupConsumerRecvErrorKind::InternalInvariant,
    ] {
        assert_eq!(
            translate_group_consumer_recv_kind(kind),
            ErrorKind::Internal
        );
    }
}
