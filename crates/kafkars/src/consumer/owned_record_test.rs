//! Public owned-record linearity and consuming-conversion API contracts.

use std::sync::Arc;

use super::{
    OwnedConsumerHeader, OwnedConsumerRecord, OwnedConsumerRecords, RetainedSourceRecord,
    TransferRejection,
};
use crate::Record;

macro_rules! assert_not_impl {
    ($type:ty: $trait:path) => {
        const _: fn() = || {
            struct Implemented;
            trait AmbiguousIfImplemented<A> {
                fn check() {}
            }
            impl<T: ?Sized> AmbiguousIfImplemented<()> for T {}
            impl<T: ?Sized + $trait> AmbiguousIfImplemented<Implemented> for T {}
            let _ = <$type as AmbiguousIfImplemented<_>>::check;
        };
    };
}

struct HeaderRef<'record> {
    _key: &'record [u8],
    _value: Option<&'record [u8]>,
}

impl<'record> HeaderRef<'record> {
    const fn new(key: &'record [u8], value: Option<&'record [u8]>) -> Self {
        Self {
            _key: key,
            _value: value,
        }
    }
}

#[test]
fn owned_record_path_is_fallible_send_linear_and_exposes_only_borrowed_bytes() {
    type HeaderCollector =
        for<'record> fn(&'record RetainedSourceRecord) -> Vec<HeaderRef<'record>>;

    fn require_send<T: Send>() {}
    fn require_error<T: std::error::Error + Send>() {}
    fn record_contract(record: &OwnedConsumerRecord) {
        let _: &str = record.topic();
        let _: i32 = record.partition();
        let _: i64 = record.offset();
        let _: Option<i64> = record.timestamp_millis();
        let _: Option<&[u8]> = record.key();
        let _: Option<&[u8]> = record.value();
        let _: Option<OwnedConsumerHeader<'_>> = record.headers().next();
    }
    fn retained_contract(record: &RetainedSourceRecord) {
        let _: &str = record.topic();
        let _: i32 = record.partition();
        let _: i64 = record.offset();
        let _: Option<i64> = record.timestamp_millis();
        let _: Option<&[u8]> = record.key();
        let _: Option<&[u8]> = record.value();
        let _: Option<OwnedConsumerHeader<'_>> = record.headers().next();
    }
    fn conversion(
        record: OwnedConsumerRecord,
        target: Arc<str>,
    ) -> Result<(Record, RetainedSourceRecord), TransferRejection> {
        record.try_into_record(target)
    }
    fn rejection_contract(rejection: TransferRejection) {
        let _: &OwnedConsumerRecord = rejection.record();
        let _: &str = rejection.target_topic();
        let _: (OwnedConsumerRecord, Arc<str>) = rejection.into_parts();
    }
    fn collect_retained_headers(record: &RetainedSourceRecord) -> Vec<HeaderRef<'_>> {
        let mut headers = Vec::new();
        for header in record.headers() {
            headers.push(HeaderRef::new(header.key(), header.value()));
        }
        headers
    }

    require_send::<OwnedConsumerRecords>();
    require_send::<OwnedConsumerRecord>();
    require_send::<RetainedSourceRecord>();
    require_error::<TransferRejection>();
    assert_not_impl!(OwnedConsumerRecords: Clone);
    assert_not_impl!(OwnedConsumerRecord: Clone);
    assert_not_impl!(OwnedConsumerRecord: Copy);
    assert_not_impl!(RetainedSourceRecord: Clone);
    assert_not_impl!(RetainedSourceRecord: Copy);
    assert_not_impl!(TransferRejection: Clone);
    assert_not_impl!(TransferRejection: Copy);
    let _ = record_contract as fn(&OwnedConsumerRecord);
    let _ = retained_contract as fn(&RetainedSourceRecord);
    let _ = conversion
        as fn(
            OwnedConsumerRecord,
            Arc<str>,
        ) -> Result<(Record, RetainedSourceRecord), TransferRejection>;
    let _ = rejection_contract as fn(TransferRejection);
    let _: HeaderCollector = collect_retained_headers;
}
