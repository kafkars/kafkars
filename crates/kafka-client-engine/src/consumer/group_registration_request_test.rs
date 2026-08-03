//! Inert dynamic classic-group registration policy ownership scenarios.

use std::{sync::Arc, time::Duration};

use super::{
    GroupConsumerClassicAssignor, GroupConsumerHandle, GroupConsumerMissingOffsetPolicy,
    GroupConsumerProtocol, GroupConsumerRegistration, GroupConsumerRegistrationErrorKind,
    group::{
        GroupConsumerShardOwner, GroupConsumerShardWake, GroupConsumerShardWakeError,
        started_group_registry_for_public_test,
    },
};
use crate::{clock::MonotonicClock, config::ConsumerReadIsolation};

#[test]
fn request_defaults_and_explicit_read_isolation_remain_owned() {
    request_defaults_and_explicit_configuration_remain_owned();
}

#[test]
fn request_defaults_and_explicit_missing_offset_policy_remain_owned() {
    request_defaults_and_explicit_configuration_remain_owned();
}

fn request_defaults_and_explicit_configuration_remain_owned() {
    let default = GroupConsumerRegistration::new(Arc::from("workers"), vec![Arc::from("orders")]);
    assert_eq!(default.protocol(), GroupConsumerProtocol::Classic);
    assert_eq!(
        default.classic_assignor(),
        Some(GroupConsumerClassicAssignor::Range)
    );
    assert_eq!(
        default.missing_offset_policy(),
        GroupConsumerMissingOffsetPolicy::Error
    );
    assert_eq!(
        default.read_isolation(),
        ConsumerReadIsolation::ReadUncommitted
    );

    let request = default
        .with_group_instance_id(Arc::from("instance-a"))
        .with_classic_assignor(GroupConsumerClassicAssignor::CooperativeSticky)
        .with_missing_offset_policy(GroupConsumerMissingOffsetPolicy::Latest)
        .with_read_isolation(ConsumerReadIsolation::ReadCommitted)
        .with_processing_timeout(Duration::from_nanos(17));
    assert_eq!(request.group(), "workers");
    assert_eq!(request.group_instance_id(), Some("instance-a"));
    assert_eq!(request.protocol(), GroupConsumerProtocol::Classic);
    assert_eq!(
        request.classic_assignor(),
        Some(GroupConsumerClassicAssignor::CooperativeSticky)
    );
    assert_eq!(
        request.missing_offset_policy(),
        GroupConsumerMissingOffsetPolicy::Latest
    );
    assert_eq!(request.topics(), &[Arc::<str>::from("orders")]);
    assert_eq!(
        request.read_isolation(),
        ConsumerReadIsolation::ReadCommitted
    );
    assert_eq!(request.processing_timeout(), Duration::from_nanos(17));

    let (
        group,
        group_instance_id,
        topics,
        protocol,
        effective_assignor,
        requested_assignor,
        missing_offset_policy,
        read_isolation,
        processing_policy,
    ) = request
        .into_validated_parts()
        .unwrap_or_else(|_request| panic!("positive representable timeout must validate"));
    assert_eq!(&*group, "workers");
    assert_eq!(group_instance_id.as_deref(), Some("instance-a"));
    assert_eq!(topics, [Arc::<str>::from("orders")]);
    assert_eq!(protocol, GroupConsumerProtocol::Classic);
    assert_eq!(
        effective_assignor,
        Some(GroupConsumerClassicAssignor::CooperativeSticky)
    );
    assert_eq!(requested_assignor, effective_assignor);
    assert_eq!(
        missing_offset_policy,
        GroupConsumerMissingOffsetPolicy::Latest
    );
    assert_eq!(read_isolation, ConsumerReadIsolation::ReadCommitted);
    assert_eq!(processing_policy.timeout_ticks(), 17);
}

#[test]
fn validated_protocol_configuration_resolves_only_compatible_classic_assignors() {
    let default = GroupConsumerRegistration::new(Arc::from("workers"), vec![Arc::from("orders")]);
    let (_, _, _, protocol, effective, requested, _, _, _) = default
        .into_validated_parts()
        .unwrap_or_else(|_request| panic!("default classic request must validate"));
    assert_eq!(protocol, GroupConsumerProtocol::Classic);
    assert_eq!(effective, Some(GroupConsumerClassicAssignor::Range));
    assert_eq!(requested, None);

    let consumer = GroupConsumerRegistration::new(Arc::from("workers"), vec![Arc::from("orders")])
        .with_protocol(GroupConsumerProtocol::Consumer);
    assert_eq!(consumer.classic_assignor(), None);
    let (_, _, _, protocol, effective, requested, _, _, _) =
        consumer.into_validated_parts().unwrap_or_else(|_request| {
            panic!("consumer request without classic assignor must validate")
        });
    assert_eq!(protocol, GroupConsumerProtocol::Consumer);
    assert_eq!(effective, None);
    assert_eq!(requested, None);
}

#[test]
fn consumer_protocol_rejects_an_explicit_classic_assignor_in_both_orders() {
    let requests = [
        GroupConsumerRegistration::new(Arc::from("workers"), vec![Arc::from("orders")])
            .with_protocol(GroupConsumerProtocol::Consumer)
            .with_classic_assignor(GroupConsumerClassicAssignor::CooperativeSticky),
        GroupConsumerRegistration::new(Arc::from("workers"), vec![Arc::from("orders")])
            .with_classic_assignor(GroupConsumerClassicAssignor::CooperativeSticky)
            .with_protocol(GroupConsumerProtocol::Consumer),
    ];

    for request in requests {
        let returned = request
            .into_validated_parts()
            .err()
            .unwrap_or_else(|| panic!("consumer protocol plus classic assignor must reject"));
        assert_eq!(returned.group(), "workers");
        assert_eq!(returned.topics(), &[Arc::<str>::from("orders")]);
        assert_eq!(returned.protocol(), GroupConsumerProtocol::Consumer);
        assert_eq!(returned.classic_assignor(), None);
        assert_eq!(
            returned
                .with_protocol(GroupConsumerProtocol::Classic)
                .classic_assignor(),
            Some(GroupConsumerClassicAssignor::CooperativeSticky)
        );
    }
}

#[test]
fn consumer_protocol_with_a_classic_assignor_is_invalid_before_registry_admission() {
    let registry = started_group_registry_for_public_test();
    let (owner, port) = GroupConsumerShardOwner::new(
        registry,
        Arc::new(MonotonicClock::new()),
        Arc::new(NoopWake),
    );
    let request =
        GroupConsumerRegistration::new(Arc::from("modern-workers"), vec![Arc::from("orders")])
            .with_protocol(GroupConsumerProtocol::Consumer)
            .with_classic_assignor(GroupConsumerClassicAssignor::CooperativeSticky);

    let error = GroupConsumerHandle::try_register(port, Arc::new(()), request)
        .err()
        .unwrap_or_else(|| panic!("consumer protocol plus classic assignor must reject"));

    assert_eq!(
        error.kind(),
        GroupConsumerRegistrationErrorKind::InvalidInput
    );
    let returned = error.into_request();
    assert_eq!(returned.group(), "modern-workers");
    assert_eq!(returned.topics(), &[Arc::<str>::from("orders")]);
    assert_eq!(returned.protocol(), GroupConsumerProtocol::Consumer);
    assert_eq!(returned.classic_assignor(), None);
    assert_eq!(owner.lock_registry_for_test().registered_group_count(), 0);
    assert_eq!(
        returned
            .with_protocol(GroupConsumerProtocol::Classic)
            .classic_assignor(),
        Some(GroupConsumerClassicAssignor::CooperativeSticky)
    );
    finish_registry(&owner);
}

#[test]
fn zero_and_unrepresentable_timeouts_return_the_exact_request() {
    for timeout in [Duration::ZERO, Duration::MAX] {
        let request =
            GroupConsumerRegistration::new(Arc::from("workers"), vec![Arc::from("orders")])
                .with_protocol(GroupConsumerProtocol::Consumer)
                .with_read_isolation(ConsumerReadIsolation::ReadCommitted)
                .with_missing_offset_policy(GroupConsumerMissingOffsetPolicy::Earliest)
                .with_processing_timeout(timeout);
        let returned = request
            .into_validated_parts()
            .err()
            .unwrap_or_else(|| panic!("invalid processing timeout must reject"));
        assert_eq!(returned.group(), "workers");
        assert_eq!(returned.topics(), &[Arc::<str>::from("orders")]);
        assert_eq!(returned.protocol(), GroupConsumerProtocol::Consumer);
        assert_eq!(returned.classic_assignor(), None);
        assert_eq!(
            returned.missing_offset_policy(),
            GroupConsumerMissingOffsetPolicy::Earliest
        );
        assert_eq!(
            returned.read_isolation(),
            ConsumerReadIsolation::ReadCommitted
        );
        assert_eq!(returned.processing_timeout(), timeout);
    }
}

#[test]
fn contended_registration_returns_the_exact_read_isolation_for_retry() {
    contended_registration_returns_the_exact_configuration_for_retry();
}

#[test]
fn contended_registration_returns_the_exact_missing_offset_policy_for_retry() {
    contended_registration_returns_the_exact_configuration_for_retry();
}

fn contended_registration_returns_the_exact_configuration_for_retry() {
    let registry = started_group_registry_for_public_test();
    let (owner, port) = GroupConsumerShardOwner::new(
        registry,
        Arc::new(MonotonicClock::new()),
        Arc::new(NoopWake),
    );
    let request = GroupConsumerRegistration::new(
        Arc::from("workers"),
        vec![Arc::from("orders"), Arc::from("payments")],
    )
    .with_group_instance_id(Arc::from("instance-a"))
    .with_classic_assignor(GroupConsumerClassicAssignor::CooperativeSticky)
    .with_missing_offset_policy(GroupConsumerMissingOffsetPolicy::Latest)
    .with_read_isolation(ConsumerReadIsolation::ReadCommitted)
    .with_processing_timeout(Duration::from_secs(41));
    let lifetime: Arc<dyn Send + Sync> = Arc::new(());
    let registry_lock = owner.lock_registry_for_test();

    let error = GroupConsumerHandle::try_register(port.clone(), Arc::clone(&lifetime), request)
        .err()
        .unwrap_or_else(|| panic!("contended registration must reject"));
    assert_eq!(error.kind(), GroupConsumerRegistrationErrorKind::Contended);
    let request = error.into_request();
    assert_eq!(request.group(), "workers");
    assert_eq!(request.group_instance_id(), Some("instance-a"));
    assert_eq!(
        request.classic_assignor(),
        Some(GroupConsumerClassicAssignor::CooperativeSticky)
    );
    assert_eq!(
        request.missing_offset_policy(),
        GroupConsumerMissingOffsetPolicy::Latest
    );
    assert_eq!(
        request.topics(),
        &[Arc::from("orders"), Arc::from("payments")]
    );
    assert_eq!(
        request.read_isolation(),
        ConsumerReadIsolation::ReadCommitted
    );
    assert_eq!(request.processing_timeout(), Duration::from_secs(41));

    drop(registry_lock);
    let handle = GroupConsumerHandle::try_register(port, lifetime, request)
        .unwrap_or_else(|error| panic!("retry registration: {error}"));
    finish(&owner, handle);
}

fn finish(owner: &GroupConsumerShardOwner, handle: GroupConsumerHandle) {
    drop(handle);
    finish_registry(owner);
}

fn finish_registry(owner: &GroupConsumerShardOwner) {
    let mut registry = owner.terminal_registry();
    registry
        .recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("registry recovery: {error}"));
    let join = registry
        .finish_shutdown()
        .unwrap_or_else(|error| panic!("registry finish: {error}"));
    drop(registry);
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("notifier join: {error}"));
}

struct NoopWake;

impl GroupConsumerShardWake for NoopWake {
    fn request_group_turn(&self) -> Result<(), GroupConsumerShardWakeError> {
        Ok(())
    }
}
