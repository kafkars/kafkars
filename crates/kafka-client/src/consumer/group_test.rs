//! Public classic-group registration and optional static-identity contract.

use std::time::Duration;

use super::{
    ClassicGroupAssignor, Consumer, ConsumerBuilder, ConsumerGroupProtocol, OffsetReset,
    ReadIsolation, RecvConsumerBatch,
};
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
            .group_protocol(ConsumerGroupProtocol::Consumer)
            .on_missing_offset(OffsetReset::Latest)
            .read_isolation(ReadIsolation::ReadCommitted)
            .processing_timeout(Duration::from_secs(41));
        let _: &str = builder.group_id();
        let _: Option<&str> = builder.selected_group_instance_id();
        let _: &[String] = builder.subscription();
        let _: ConsumerGroupProtocol = builder.selected_group_protocol();
        let _: Option<ClassicGroupAssignor> = builder.selected_classic_group_assignor();
        let _: OffsetReset = builder.offset_reset();
        let _: ReadIsolation = builder.selected_read_isolation();
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
fn classic_assignor_is_scoped_to_the_classic_protocol() {
    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("lazy client start: {error}"));
    assert_eq!(
        client
            .consumer("range-workers")
            .selected_classic_group_assignor(),
        Some(ClassicGroupAssignor::Range)
    );
    assert_eq!(
        client
            .consumer("cooperative-workers")
            .classic_group_assignor(ClassicGroupAssignor::CooperativeSticky)
            .selected_classic_group_assignor(),
        Some(ClassicGroupAssignor::CooperativeSticky)
    );
    assert_eq!(
        client
            .consumer("modern-workers")
            .group_protocol(ConsumerGroupProtocol::Consumer)
            .selected_classic_group_assignor(),
        None
    );
}

#[test]
fn classic_protocol_is_default_and_consumer_protocol_is_explicit() {
    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("lazy client start: {error}"));
    assert_eq!(
        client.consumer("workers").selected_group_protocol(),
        ConsumerGroupProtocol::Classic
    );
    assert_eq!(
        client
            .consumer("workers")
            .group_protocol(ConsumerGroupProtocol::Consumer)
            .selected_group_protocol(),
        ConsumerGroupProtocol::Consumer
    );
}

#[test]
fn consumer_protocol_rejects_an_explicit_classic_assignor_in_both_orders() {
    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("lazy client start: {error}"));
    let builders = [
        client
            .consumer("modern-workers")
            .group_protocol(ConsumerGroupProtocol::Consumer)
            .classic_group_assignor(ClassicGroupAssignor::CooperativeSticky),
        client
            .consumer("modern-workers")
            .classic_group_assignor(ClassicGroupAssignor::CooperativeSticky)
            .group_protocol(ConsumerGroupProtocol::Consumer),
    ];

    for builder in builders {
        let rejected = builder
            .subscribe(["orders"])
            .build()
            .err()
            .unwrap_or_else(|| panic!("consumer protocol plus classic assignor must reject"));
        assert_eq!(rejected.error().kind(), ErrorKind::Configuration);
        let (builder, error) = rejected.into_parts();
        assert_eq!(error.kind(), ErrorKind::Configuration);
        assert_eq!(builder.group_id(), "modern-workers");
        assert_eq!(builder.subscription(), ["orders"]);
        assert_eq!(
            builder.selected_group_protocol(),
            ConsumerGroupProtocol::Consumer
        );
        assert_eq!(builder.selected_classic_group_assignor(), None);
        assert_eq!(
            builder
                .group_protocol(ConsumerGroupProtocol::Classic)
                .selected_classic_group_assignor(),
            Some(ClassicGroupAssignor::CooperativeSticky)
        );
    }
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
        .classic_group_assignor(ClassicGroupAssignor::CooperativeSticky)
        .on_missing_offset(OffsetReset::Latest)
        .read_isolation(ReadIsolation::ReadCommitted)
        .processing_timeout(Duration::from_secs(41))
        .build()
        .err()
        .unwrap_or_else(|| panic!("empty static identity must reject"));
    assert_eq!(rejected.error().kind(), ErrorKind::Configuration);
    assert_eq!(rejected.builder().group_id(), "static-workers");
    assert_eq!(rejected.builder().selected_group_instance_id(), Some(""));
    assert_eq!(rejected.builder().subscription(), ["orders"]);
    assert_eq!(
        rejected.builder().selected_classic_group_assignor(),
        Some(ClassicGroupAssignor::CooperativeSticky)
    );
    assert_eq!(rejected.builder().offset_reset(), OffsetReset::Latest);
    assert_eq!(
        rejected.builder().selected_read_isolation(),
        ReadIsolation::ReadCommitted
    );
    assert_eq!(
        rejected.builder().selected_processing_timeout(),
        Duration::from_secs(41)
    );
}

#[test]
fn offset_reset_defaults_to_error_and_retains_each_explicit_choice() {
    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("lazy client start: {error}"));
    assert_eq!(
        client.consumer("default-workers").offset_reset(),
        OffsetReset::Error
    );

    for policy in [
        OffsetReset::Error,
        OffsetReset::Earliest,
        OffsetReset::Latest,
    ] {
        assert_eq!(
            client
                .consumer("explicit-workers")
                .on_missing_offset(policy)
                .offset_reset(),
            policy
        );
    }
}

#[test]
fn read_isolation_defaults_to_uncommitted_and_retains_each_explicit_choice() {
    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("lazy client start: {error}"));
    assert_eq!(
        client.consumer("default-workers").selected_read_isolation(),
        ReadIsolation::ReadUncommitted
    );

    for isolation in [ReadIsolation::ReadUncommitted, ReadIsolation::ReadCommitted] {
        assert_eq!(
            client
                .consumer("explicit-workers")
                .read_isolation(isolation)
                .selected_read_isolation(),
            isolation
        );
    }
}
