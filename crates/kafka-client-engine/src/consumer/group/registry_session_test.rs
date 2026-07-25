//! Registry-selected session replacement and close fencing scenarios.

use std::sync::Arc;

use kafka_client_core::{AssignmentGeneration, GroupId};

use super::{
    registry_close::GroupConsumerCloseError,
    registry_entry::GroupConsumerEntrySessionError,
    registry_session::GroupConsumerSessionFailure,
    registry_test_support::{register, started_registry, stop_registry},
};

#[test]
fn session_replacement_is_selected_by_nonreused_group_identity() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    let generation = AssignmentGeneration::try_from_raw(1)
        .unwrap_or_else(|| panic!("assignment generation must be nonzero"));

    registry
        .prepare_session_replacement(group_id, Arc::from("member"), 1, generation, Vec::new())
        .unwrap_or_else(|error| panic!("session replacement failed: {error:?}"))
        .commit();
    assert_eq!(
        registry
            .entry(group_id)
            .and_then(|entry| entry.catalog.assignment_generation()),
        Some(generation)
    );
    stop_registry(&mut registry);
}

#[test]
fn unknown_and_closing_groups_cannot_replace_sessions() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    let unknown =
        GroupId::try_from_raw(999).unwrap_or_else(|| panic!("unknown identity must be nonzero"));
    let generation = AssignmentGeneration::try_from_raw(1)
        .unwrap_or_else(|| panic!("assignment generation must be nonzero"));

    assert!(matches!(
        registry.prepare_session_replacement(
            unknown,
            Arc::from("member"),
            1,
            generation,
            Vec::new(),
        ),
        Err(GroupConsumerSessionFailure::UnknownGroup)
    ));
    assert_eq!(registry.close_group(group_id), Ok(()));
    assert_eq!(
        registry.close_group(group_id),
        Err(GroupConsumerCloseError::AlreadyClosing)
    );
    assert!(matches!(
        registry.prepare_session_replacement(
            group_id,
            Arc::from("member"),
            1,
            generation,
            Vec::new(),
        ),
        Err(GroupConsumerSessionFailure::Entry(
            GroupConsumerEntrySessionError::Closing
        ))
    ));
    stop_registry(&mut registry);
}
