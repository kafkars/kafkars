//! Public unique share-consumer ownership contract.

use super::{ShareConsumer, ShareConsumerAssignment, ShareConsumerBuilder};

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
fn builder_and_unique_handle_expose_membership_without_normal_consumer_controls() {
    fn require_send<T: Send>() {}
    fn builder_contract(builder: ShareConsumerBuilder) {
        let _ = builder.subscribe(["orders"]);
    }
    fn handle_contract(consumer: &ShareConsumer) {
        let _: &str = consumer.group_id();
        let _: Option<&str> = consumer.rack();
        let _: &[String] = consumer.subscription();
        let _: Option<crate::KafkaError> = consumer.startup_error();
        let _: Result<Option<ShareConsumerAssignment>, crate::KafkaError> = consumer.assignment();
    }

    require_send::<ShareConsumer>();
    assert_not_impl!(ShareConsumer: Clone);
    assert_not_impl!(ShareConsumer: Sync);
    let _ = builder_contract as fn(ShareConsumerBuilder);
    let _ = handle_contract as fn(&ShareConsumer);
}
