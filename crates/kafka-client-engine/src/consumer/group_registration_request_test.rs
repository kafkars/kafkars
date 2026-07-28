//! Inert dynamic classic-group registration policy ownership scenarios.

use std::{sync::Arc, time::Duration};

use super::{
    GroupConsumerHandle, GroupConsumerMissingOffsetPolicy, GroupConsumerRegistration,
    GroupConsumerRegistrationErrorKind,
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
        .with_missing_offset_policy(GroupConsumerMissingOffsetPolicy::Latest)
        .with_read_isolation(ConsumerReadIsolation::ReadCommitted)
        .with_processing_timeout(Duration::from_nanos(17));
    assert_eq!(request.group(), "workers");
    assert_eq!(request.group_instance_id(), Some("instance-a"));
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
        missing_offset_policy,
        read_isolation,
        processing_policy,
    ) = request
        .into_validated_parts()
        .unwrap_or_else(|_request| panic!("positive representable timeout must validate"));
    assert_eq!(&*group, "workers");
    assert_eq!(group_instance_id.as_deref(), Some("instance-a"));
    assert_eq!(topics, [Arc::<str>::from("orders")]);
    assert_eq!(
        missing_offset_policy,
        GroupConsumerMissingOffsetPolicy::Latest
    );
    assert_eq!(read_isolation, ConsumerReadIsolation::ReadCommitted);
    assert_eq!(processing_policy.timeout_ticks(), 17);
}

#[test]
fn zero_and_unrepresentable_timeouts_return_the_exact_request() {
    for timeout in [Duration::ZERO, Duration::MAX] {
        let request =
            GroupConsumerRegistration::new(Arc::from("workers"), vec![Arc::from("orders")])
                .with_read_isolation(ConsumerReadIsolation::ReadCommitted)
                .with_missing_offset_policy(GroupConsumerMissingOffsetPolicy::Earliest)
                .with_processing_timeout(timeout);
        let returned = request
            .into_validated_parts()
            .err()
            .unwrap_or_else(|| panic!("invalid processing timeout must reject"));
        assert_eq!(returned.group(), "workers");
        assert_eq!(returned.topics(), &[Arc::<str>::from("orders")]);
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
