//! Public share batch record views and linear ownership contracts.

use super::{ShareConsumerBatch, ShareConsumerHeader};
use crate::consumer::share::registry_delivery_test::{finish, staged_handle};

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
fn batch_is_unique_send_ownership_with_borrowed_acquisition_views() {
    fn require_send<T: Send>() {}
    require_send::<ShareConsumerBatch>();
    assert_not_impl!(ShareConsumerBatch: Clone);

    let (owner, mut handle, group_id) = staged_handle();
    let batch = handle
        .try_take_batch()
        .unwrap_or_else(|error| panic!("take batch: {error}"))
        .unwrap_or_else(|| panic!("staged batch"));
    assert_eq!(batch.partition_count(), 1);
    assert_eq!(batch.acquisition_count(), 1);
    assert_eq!(batch.record_count(), 1);
    let record = batch
        .records()
        .next()
        .unwrap_or_else(|| panic!("share record"));
    assert_eq!(record.topic(), "events");
    assert_eq!(record.partition(), 0);
    assert_eq!(record.offset(), 41);
    assert_eq!(record.timestamp_millis(), Some(20));
    assert_eq!(record.key(), None);
    assert_eq!(record.value(), Some(b"value".as_slice()));
    assert_eq!(record.delivery_count(), 1);
    assert_eq!(record.headers().count(), 0);
    assert!(record.belongs_to(batch.delivery()));
    assert_eq!(record.ordinal(), 0);
    assert_eq!(record.acquisition().range().first_offset(), 41);
    drop(batch);
    finish(owner, group_id);
}

#[test]
fn header_parts_retain_the_record_lifetime_after_the_view_is_consumed() {
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
