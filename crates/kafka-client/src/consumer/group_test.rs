//! Public classic-group registration and optional static-identity contract.

use std::time::Duration;

use super::{Consumer, ConsumerBuilder, RecvConsumerBatch};
use crate::{Client, ErrorKind};

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
fn builder_and_unique_handle_expose_static_identity_without_control_capabilities() {
    fn require_send<T: Send>() {}
    fn builder_contract(builder: ConsumerBuilder) {
        let builder = builder
            .group_instance_id("instance-a")
            .subscribe(["orders"])
            .processing_timeout(Duration::from_secs(41));
        let _: &str = builder.group_id();
        let _: Option<&str> = builder.selected_group_instance_id();
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

#[test]
fn static_identity_is_opt_in_and_exactly_recovered_on_invalid_registration() {
    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("lazy client start: {error}"));
    assert_eq!(
        client
            .consumer("dynamic-workers")
            .selected_group_instance_id(),
        None
    );
    let builder = client
        .consumer("static-workers")
        .group_instance_id("instance-a");
    assert_eq!(builder.selected_group_instance_id(), Some("instance-a"));

    let rejected = client
        .consumer("static-workers")
        .group_instance_id("")
        .subscribe(["orders"])
        .processing_timeout(Duration::from_secs(41))
        .build()
        .err()
        .unwrap_or_else(|| panic!("empty static identity must reject"));
    assert_eq!(rejected.error().kind(), ErrorKind::Configuration);
    assert_eq!(rejected.builder().group_id(), "static-workers");
    assert_eq!(rejected.builder().selected_group_instance_id(), Some(""));
    assert_eq!(rejected.builder().subscription(), ["orders"]);
    assert_eq!(
        rejected.builder().selected_processing_timeout(),
        Duration::from_secs(41)
    );
}
