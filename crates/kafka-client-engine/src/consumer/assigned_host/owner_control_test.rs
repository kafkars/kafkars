//! Named control-target translation scenarios.

use std::time::Duration;

use kafka_client_core::{AssignedConsumerEffect, NextFetchOffset, StartPosition};

use super::super::{
    assigned_owner_effect::FrontEffect,
    assigned_owner_model::AssignedConsumerOwnerError,
    assigned_owner_test::{input, owner},
};
use super::{AssignedConsumerControlInputError, AssignedConsumerPartition};

#[test]
fn named_pause_uses_the_catalog_identity_then_core_epoch_policy() {
    let mut owner = owner(1);
    let epoch = owner
        .replace_assignment(
            vec![input(
                "orders",
                0,
                StartPosition::Offset(nonnegative_offset(4)),
            )],
            Duration::from_secs(1),
        )
        .unwrap_or_else(|error| panic!("assign: {error:?}"));
    assert_eq!(owner.interpret_front_effect(), FrontEffect::Interpreted);
    let target = AssignedConsumerPartition::try_new("orders", 0)
        .unwrap_or_else(|error| panic!("target: {error}"));

    owner
        .pause_named(epoch, &target)
        .unwrap_or_else(|error| panic!("pause: {error:?}"));

    assert!(matches!(
        owner.effects.front(),
        Some(AssignedConsumerEffect::Suspend { .. })
    ));
}

#[test]
fn unknown_name_rejects_without_creating_a_core_control_effect() {
    let mut owner = owner(1);
    let epoch = owner
        .replace_assignment(
            vec![input(
                "orders",
                0,
                StartPosition::Offset(nonnegative_offset(4)),
            )],
            Duration::from_secs(1),
        )
        .unwrap_or_else(|error| panic!("assign: {error:?}"));
    assert_eq!(owner.interpret_front_effect(), FrontEffect::Interpreted);
    let target = AssignedConsumerPartition::try_new("missing", 0)
        .unwrap_or_else(|error| panic!("target: {error}"));

    let error = owner
        .pause_named(epoch, &target)
        .err()
        .unwrap_or_else(|| panic!("unknown topic must reject"));

    assert_eq!(
        error,
        AssignedConsumerOwnerError::ControlInput(AssignedConsumerControlInputError::UnknownTopic)
    );
    assert!(owner.effects.is_empty());
}

fn nonnegative_offset(value: i64) -> NextFetchOffset {
    NextFetchOffset::try_from_raw(value).unwrap_or_else(|| panic!("nonnegative test offset"))
}
