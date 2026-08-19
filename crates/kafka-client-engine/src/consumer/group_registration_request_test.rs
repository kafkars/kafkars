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
use crate::{
    clock::MonotonicClock,
    config::{
        ConsumerReadIsolation, EngineClassicGroupConfig, EngineConsumerFetchConfig,
        EngineConsumerLimits, EngineGroupConsumerOperationConfig,
    },
};

#[test]
fn request_defaults_and_explicit_read_isolation_remain_owned() {
    request_defaults_and_explicit_configuration_remain_owned();
}

#[test]
fn request_defaults_and_explicit_missing_offset_policy_remain_owned() {
    request_defaults_and_explicit_configuration_remain_owned();
}

#[expect(
    clippy::too_many_lines,
    reason = "the fixture compares every default and explicit registration field in one ownership matrix"
)]
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
    assert_eq!(default.processing_timeout(), Duration::from_secs(300));
    assert_eq!(
        default.classic_group_config(),
        EngineClassicGroupConfig::default()
    );
    assert_eq!(
        default.operation_config(),
        EngineGroupConsumerOperationConfig::default()
    );
    assert_eq!(default.fetch(), EngineConsumerFetchConfig::default());
    assert_eq!(default.limits(), EngineConsumerLimits::default());

    let fetch = EngineConsumerFetchConfig::new(
        Duration::from_millis(125),
        4_096,
        2 * 1024 * 1024,
        512 * 1024,
        Duration::from_secs(7),
    );
    let limits = EngineConsumerLimits::new(3, 5, 4 * 1024 * 1024, 512 * 1024);
    let classic = EngineClassicGroupConfig::new(
        Duration::from_secs(11),
        Duration::from_secs(31),
        Duration::from_secs(4),
        Duration::from_secs(12),
        Duration::from_secs(2),
        Duration::from_secs(32),
    );
    let operations =
        EngineGroupConsumerOperationConfig::new(Duration::from_secs(13), Duration::from_secs(17));
    let explicit = GroupConsumerRegistration::new(Arc::from("workers"), vec![Arc::from("orders")])
        .with_group_instance_id(Arc::from("instance-a"))
        .with_classic_assignor(GroupConsumerClassicAssignor::CooperativeSticky)
        .with_missing_offset_policy(GroupConsumerMissingOffsetPolicy::Latest)
        .with_read_isolation(ConsumerReadIsolation::ReadCommitted)
        .with_processing_timeout(Duration::from_nanos(17))
        .with_classic_group_config(classic)
        .with_operation_config(operations)
        .with_fetch(fetch)
        .with_limits(limits);
    assert_eq!(explicit.group(), "workers");
    assert_eq!(explicit.group_instance_id(), Some("instance-a"));
    assert_eq!(explicit.topics(), &[Arc::<str>::from("orders")]);
    assert_eq!(explicit.protocol(), GroupConsumerProtocol::Classic);
    assert_eq!(
        explicit.classic_assignor(),
        Some(GroupConsumerClassicAssignor::CooperativeSticky)
    );
    assert_eq!(
        explicit.missing_offset_policy(),
        GroupConsumerMissingOffsetPolicy::Latest
    );
    assert_eq!(
        explicit.read_isolation(),
        ConsumerReadIsolation::ReadCommitted
    );
    assert_eq!(explicit.processing_timeout(), Duration::from_nanos(17));
    assert_eq!(explicit.classic_group_config(), classic);
    assert_eq!(explicit.operation_config(), operations);
    assert_eq!(explicit.fetch(), fetch);
    assert_eq!(explicit.limits(), limits);
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
        raw_classic,
        validated_classic,
        raw_operations,
        validated_operations,
        raw_fetch,
        validated_fetch,
        raw_limits,
        validated_limits,
    ) = explicit
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
    assert_eq!(raw_classic, classic);
    assert_eq!(validated_classic.timing().session_timeout_ms(), 11_000);
    assert_eq!(validated_classic.timing().rebalance_timeout_ms(), 31_000);
    assert_eq!(
        validated_classic.heartbeat().interval_ticks(),
        4_000_000_000
    );
    assert_eq!(validated_classic.rejoin().backoff_ticks(), 2_000_000_000);
    assert_eq!(raw_operations, operations);
    assert_eq!(validated_operations.seek_timeout(), Duration::from_secs(13));
    assert_eq!(
        validated_operations.close_timeout(),
        Duration::from_secs(17)
    );
    assert_eq!(raw_fetch, fetch);
    assert_eq!(validated_fetch.max_wait_ms(), 125);
    assert_eq!(raw_limits, limits);
    assert_eq!(validated_limits.in_flight_fetches(), 3);
    assert_eq!(validated_limits.buffered_batches(), 5);
    assert_eq!(validated_limits.buffered_bytes(), 4 * 1024 * 1024);
    assert_eq!(validated_limits.max_batch_bytes(), 512 * 1024);
}

#[test]
fn validated_protocol_configuration_resolves_only_compatible_classic_assignors() {
    let default = GroupConsumerRegistration::new(Arc::from("workers"), vec![Arc::from("orders")]);
    let (_, _, _, protocol, effective, requested, _, _, _, _, _, _, _, _, _, _, _) = default
        .into_validated_parts()
        .unwrap_or_else(|_request| panic!("default classic request must validate"));
    assert_eq!(protocol, GroupConsumerProtocol::Classic);
    assert_eq!(effective, Some(GroupConsumerClassicAssignor::Range));
    assert_eq!(requested, None);

    let consumer = GroupConsumerRegistration::new(Arc::from("workers"), vec![Arc::from("orders")])
        .with_protocol(GroupConsumerProtocol::Consumer);
    assert_eq!(consumer.classic_assignor(), None);
    let (_, _, _, protocol, effective, requested, _, _, _, _, _, _, _, _, _, _, _) =
        consumer.into_validated_parts().unwrap_or_else(|_request| {
            panic!("consumer request without classic assignor must validate")
        });
    assert_eq!(protocol, GroupConsumerProtocol::Consumer);
    assert_eq!(effective, None);
    assert_eq!(requested, None);
}

#[test]
fn invalid_or_modern_protocol_classic_timing_returns_the_exact_registration() {
    let default = EngineClassicGroupConfig::default();
    let invalid = EngineClassicGroupConfig::new(
        Duration::ZERO,
        default.rebalance_timeout(),
        default.heartbeat_interval(),
        default.heartbeat_attempt_timeout(),
        default.rejoin_backoff(),
        default.rejoin_attempt_timeout(),
    );
    let request = GroupConsumerRegistration::new(Arc::from("workers"), vec![Arc::from("orders")])
        .with_classic_group_config(invalid);
    let returned = request
        .into_validated_parts()
        .err()
        .unwrap_or_else(|| panic!("zero classic session timeout must reject"));
    assert_eq!(returned.classic_group_config(), invalid);

    let custom = EngineClassicGroupConfig::new(
        Duration::from_secs(11),
        default.rebalance_timeout(),
        default.heartbeat_interval(),
        default.heartbeat_attempt_timeout(),
        default.rejoin_backoff(),
        default.rejoin_attempt_timeout(),
    );
    let request = GroupConsumerRegistration::new(Arc::from("workers"), vec![Arc::from("orders")])
        .with_protocol(GroupConsumerProtocol::Consumer)
        .with_classic_group_config(custom);
    let returned = request
        .into_validated_parts()
        .err()
        .unwrap_or_else(|| panic!("modern protocol cannot consume classic timing"));
    assert_eq!(returned.classic_group_config(), custom);
}

#[test]
fn invalid_fetch_policy_returns_the_exact_registration() {
    let default = EngineConsumerFetchConfig::default();
    let fetch = EngineConsumerFetchConfig::new(
        Duration::ZERO,
        default.min_bytes(),
        default.max_bytes(),
        default.partition_max_bytes(),
        default.attempt_timeout(),
    );
    let request = GroupConsumerRegistration::new(Arc::from("workers"), vec![Arc::from("orders")])
        .with_fetch(fetch);
    let returned = request
        .into_validated_parts()
        .err()
        .unwrap_or_else(|| panic!("invalid Fetch policy must reject"));
    assert_eq!(returned.fetch(), fetch);
}

#[test]
fn invalid_operation_duration_returns_the_exact_registration() {
    for operations in [
        EngineGroupConsumerOperationConfig::new(Duration::ZERO, Duration::from_secs(17)),
        EngineGroupConsumerOperationConfig::new(Duration::from_secs(13), Duration::MAX),
    ] {
        let request =
            GroupConsumerRegistration::new(Arc::from("workers"), vec![Arc::from("orders")])
                .with_operation_config(operations);
        let returned = request
            .into_validated_parts()
            .err()
            .unwrap_or_else(|| panic!("invalid operation duration must reject"));
        assert_eq!(returned.operation_config(), operations);
    }
}

#[test]
fn invalid_or_fetch_incoherent_limits_return_the_exact_registration() {
    let invalid = EngineConsumerLimits::new(0, 1, 1024 * 1024, 1024 * 1024);
    let request = GroupConsumerRegistration::new(Arc::from("workers"), vec![Arc::from("orders")])
        .with_limits(invalid);
    let returned = request
        .into_validated_parts()
        .err()
        .unwrap_or_else(|| panic!("zero Fetch-call capacity must reject"));
    assert_eq!(returned.limits(), invalid);

    let incoherent = EngineConsumerLimits::new(1, 1, 1024 * 1024, 512 * 1024);
    let request = GroupConsumerRegistration::new(Arc::from("workers"), vec![Arc::from("orders")])
        .with_limits(incoherent);
    let returned = request
        .into_validated_parts()
        .err()
        .unwrap_or_else(|| panic!("batch ceiling below partition Fetch bytes must reject"));
    assert_eq!(returned.limits(), incoherent);
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
    let fetch = EngineConsumerFetchConfig::new(
        Duration::from_millis(125),
        4_096,
        2 * 1024 * 1024,
        512 * 1024,
        Duration::from_secs(7),
    );
    let limits = EngineConsumerLimits::new(3, 5, 4 * 1024 * 1024, 512 * 1024);
    let classic_group_config = EngineClassicGroupConfig::new(
        Duration::from_secs(11),
        Duration::from_secs(31),
        Duration::from_secs(4),
        Duration::from_secs(12),
        Duration::from_secs(2),
        Duration::from_secs(32),
    );
    let operation_config =
        EngineGroupConsumerOperationConfig::new(Duration::from_secs(13), Duration::from_secs(17));
    let request = GroupConsumerRegistration::new(
        Arc::from("workers"),
        vec![Arc::from("orders"), Arc::from("payments")],
    )
    .with_group_instance_id(Arc::from("instance-a"))
    .with_classic_assignor(GroupConsumerClassicAssignor::CooperativeSticky)
    .with_missing_offset_policy(GroupConsumerMissingOffsetPolicy::Latest)
    .with_read_isolation(ConsumerReadIsolation::ReadCommitted)
    .with_processing_timeout(Duration::from_secs(41))
    .with_classic_group_config(classic_group_config)
    .with_operation_config(operation_config)
    .with_fetch(fetch)
    .with_limits(limits);
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
    assert_eq!(request.classic_group_config(), classic_group_config);
    assert_eq!(request.operation_config(), operation_config);
    assert_eq!(request.fetch(), fetch);
    assert_eq!(request.limits(), limits);

    drop(registry_lock);
    let handle = GroupConsumerHandle::try_register(port, lifetime, request)
        .unwrap_or_else(|error| panic!("retry registration: {error}"));
    assert_eq!(handle.seek_timeout, Duration::from_secs(13));
    assert_eq!(handle.close_timeout, Duration::from_secs(17));
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
