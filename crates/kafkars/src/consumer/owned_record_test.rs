//! Public owned-record linearity and consuming-conversion API contracts.

use std::sync::Arc;

use super::{OwnedConsumerHeader, OwnedConsumerRecord, OwnedConsumerRecords};
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

#[test]
fn owned_record_path_is_send_linear_and_exposes_only_borrowed_bytes() {
    fn require_send<T: Send>() {}
    fn record_contract(record: &OwnedConsumerRecord) {
        let _: &str = record.topic();
        let _: i32 = record.partition();
        let _: i64 = record.offset();
        let _: Option<i64> = record.timestamp_millis();
        let _: Option<&[u8]> = record.key();
        let _: Option<&[u8]> = record.value();
        let _: Option<OwnedConsumerHeader<'_>> = record.headers().next();
    }
    fn conversion(record: OwnedConsumerRecord) -> Record {
        record.into_record(Arc::<str>::from("destination"))
    }

    require_send::<OwnedConsumerRecords>();
    require_send::<OwnedConsumerRecord>();
    assert_not_impl!(OwnedConsumerRecords: Clone);
    assert_not_impl!(OwnedConsumerRecord: Clone);
    assert_not_impl!(OwnedConsumerRecord: Copy);
    let _ = record_contract as fn(&OwnedConsumerRecord);
    let _ = conversion as fn(OwnedConsumerRecord) -> Record;
}
