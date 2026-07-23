//! Shard terminal settlement and notifier-owner transfer scenarios.

use std::time::Instant;

use kafka_client_core::{Deadline, Moment};

use crate::{
    clock::OperationDeadline,
    completion::CompletionRegistryError,
    producer::{
        admission_test::record,
        host_limits_test::{start, valid_limits},
    },
};

use super::data::ProducerShardData;

#[test]
fn settled_shard_transfers_the_notifier_join_owner() {
    let mut data = ProducerShardData::new(start(valid_limits()));
    let accepted = data
        .try_admit_explicit(Moment::from_tick(1), deadline(), record("accepted"))
        .unwrap_or_else(|error| panic!("record should enter core: {error:?}"));

    data.execution_unavailable(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("accepted work should settle: {error}"));
    assert!(accepted.into_delivery_observer().wait().is_err());
    data.verify_release_before_completion()
        .unwrap_or_else(|error| panic!("settled resources should release: {error}"));
    data.drain_terminal_mechanisms();
    data.verify_terminal_cleanup()
        .unwrap_or_else(|error| panic!("terminal resources should be empty: {error}"));

    let notifier = data
        .begin_notification_shutdown()
        .unwrap_or_else(|error| panic!("settled notifier should stop: {error}"));
    assert_eq!(notifier.join_off_notifier(), Ok(()));
}

#[test]
fn damaged_recovery_retains_notifier_ownership_with_the_error() {
    let mut data = ProducerShardData::new(start(valid_limits()));
    let _accepted = data
        .try_admit_explicit(Moment::from_tick(1), deadline(), record("accepted"))
        .unwrap_or_else(|error| panic!("record should enter core: {error:?}"));

    let recovery = data.recover_notifier();
    assert_eq!(
        recovery.error,
        Some(CompletionRegistryError::UnsettledCompletion)
    );
    let notifier = recovery
        .notifier
        .unwrap_or_else(|| panic!("recovery must retain the notifier join owner"));
    assert_eq!(notifier.join_off_notifier(), Ok(()));
}

fn deadline() -> OperationDeadline {
    OperationDeadline::from_parts_for_test(Deadline::from_tick(90), Instant::now())
}
