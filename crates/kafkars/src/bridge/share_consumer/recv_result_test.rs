//! Exhaustive facade mapping for share receive failures.

use kafka_client_engine::share::ShareConsumerRecvErrorKind;

use crate::ErrorKind;

use super::batch::ShareConsumerHeader;
use super::recv_result::translate_share_consumer_recv_kind;

#[test]
fn share_header_parts_retain_the_record_lifetime_after_translation() {
    type HeaderParts<'record> = (&'record [u8], Option<&'record [u8]>);
    type HeaderContract = for<'record> fn(ShareConsumerHeader<'record>) -> HeaderParts<'record>;

    #[expect(
        clippy::needless_pass_by_value,
        reason = "consuming the iterator item proves returned references retain the record lifetime"
    )]
    fn consume(header: ShareConsumerHeader<'_>) -> HeaderParts<'_> {
        let key = header.key();
        let value = header.value();
        (key, value)
    }

    let _: HeaderContract = consume;
}

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
