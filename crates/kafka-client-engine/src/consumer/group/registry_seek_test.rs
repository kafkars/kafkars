//! Assignment translation and pre-reserved group-seek registry tests.

use std::{sync::Arc, time::Duration};

use kafka_client_core::{NextFetchOffset, StartPosition};

use crate::{
    clock::MonotonicClock,
    consumer::{
        group_control::GroupConsumerPartition,
        group_seek::{
            GroupConsumerSeekCompletion, GroupConsumerSeekCompletionObservation,
            GroupConsumerSeekTerminal,
        },
    },
};

use super::{
    registry_seek::GroupConsumerSeekRegistryError,
    registry_test_support::{
        install_ready_group_delivery, install_session, register, started_registry, stop_registry,
    },
};

#[test]
fn assignment_absence_rejects_before_completion_or_fetch_mutation() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    let completion = Arc::new(GroupConsumerSeekCompletion::pending());

    assert_eq!(
        registry.seek_partition(
            group_id,
            target(),
            StartPosition::Beginning,
            capture(),
            Arc::clone(&completion),
        ),
        Err(GroupConsumerSeekRegistryError::NoAssignment)
    );
    assert_eq!(
        completion.observe(),
        GroupConsumerSeekCompletionObservation::Pending
    );
    stop_registry(&mut registry);
}

#[test]
fn current_assignment_translation_commits_explicit_offset_terminal() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    install_session(&mut registry, group_id);
    install_ready_group_delivery(&mut registry, group_id, 17);
    let completion = Arc::new(GroupConsumerSeekCompletion::pending());

    registry
        .seek_partition(
            group_id,
            target(),
            StartPosition::Offset(
                NextFetchOffset::try_from_raw(42).unwrap_or_else(|| panic!("offset")),
            ),
            capture(),
            Arc::clone(&completion),
        )
        .unwrap_or_else(|error| panic!("seek: {error:?}"));

    assert_eq!(
        completion.observe(),
        GroupConsumerSeekCompletionObservation::Terminal(GroupConsumerSeekTerminal::Succeeded)
    );
    stop_registry(&mut registry);
}

fn target() -> GroupConsumerPartition {
    GroupConsumerPartition::try_new("orders", 0)
        .unwrap_or_else(|error| panic!("partition: {error}"))
}

fn capture() -> crate::clock::DeadlineCapture {
    MonotonicClock::new()
        .capture_deadline_after(Duration::from_secs(30))
        .unwrap_or_else(|error| panic!("capture: {error:?}"))
}
