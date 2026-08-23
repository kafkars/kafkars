//! Exhaustive facade mapping for share receive failures.

use kafka_client_engine::share::ShareConsumerRecvErrorKind;

use crate::ErrorKind;

use super::recv_result::translate_share_consumer_recv_kind;

#[test]
fn every_share_receive_failure_maps_to_internal_without_retry_authority() {
    for kind in [
        ShareConsumerRecvErrorKind::HostUnavailable,
        ShareConsumerRecvErrorKind::InternalInvariant,
    ] {
        let error = translate_share_consumer_recv_kind(kind);
        assert_eq!(error.kind(), ErrorKind::Internal);
        assert_eq!(error.retry_advice(), crate::RetryAdvice::DoNotRetry);
        assert_eq!(error.delivery_status(), None);
    }
}
