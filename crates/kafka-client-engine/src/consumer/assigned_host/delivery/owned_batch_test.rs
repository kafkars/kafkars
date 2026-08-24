//! Public shape and linearity evidence for the owned direct-consumer batch.

use super::{AssignedConsumerBatch, AssignedConsumerOwnedBatch, AssignedConsumerOwnedRecords};

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
fn owned_batch_is_send_linear_and_consumes_into_bounded_records() {
    fn require_send<T: Send>() {}
    fn require_conversion(_convert: fn(AssignedConsumerBatch) -> AssignedConsumerOwnedBatch) {}
    fn require_records(_records: fn(AssignedConsumerOwnedBatch) -> AssignedConsumerOwnedRecords) {}

    require_send::<AssignedConsumerOwnedBatch>();
    require_conversion(AssignedConsumerBatch::into_owned);
    require_records(AssignedConsumerOwnedBatch::into_records);
    assert_not_impl!(AssignedConsumerOwnedBatch: Clone);
    assert_not_impl!(AssignedConsumerOwnedBatch: Copy);
}
