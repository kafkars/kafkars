//! Public dynamic classic-group registration ownership contract.

use std::time::Duration;

use super::{Consumer, ConsumerBuilder, RecvConsumerBatch};

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
fn builder_and_unique_handle_expose_only_base_dynamic_group_capabilities() {
    fn require_send<T: Send>() {}
    fn builder_contract(builder: ConsumerBuilder) {
        let builder = builder
            .subscribe(["orders"])
            .processing_timeout(Duration::from_secs(41));
        let _: &str = builder.group_id();
        let _: &[String] = builder.subscription();
        let _: Duration = builder.selected_processing_timeout();
    }
    fn handle_contract(consumer: &mut Consumer) {
        let _: &str = consumer.group_id();
        let _: &[String] = consumer.subscription();
        drop::<RecvConsumerBatch<'_>>(consumer.recv());
    }

    require_send::<Consumer>();
    assert_not_impl!(Consumer: Clone);
    assert_not_impl!(Consumer: Sync);
    let _ = builder_contract as fn(ConsumerBuilder);
    let _ = handle_contract as fn(&mut Consumer);
}
