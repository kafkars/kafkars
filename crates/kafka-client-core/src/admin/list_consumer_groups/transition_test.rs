//! State-machine scenarios for discovery, filtering, merge, and exact errors.

use core::num::NonZeroI16;

use crate::{Deadline, Moment, OperationId};

use super::{
    AdminConsumerGroupListing, AdminListConsumerGroupsBrokerError,
    AdminListConsumerGroupsBrokerOutcome, AdminListConsumerGroupsEffect,
    AdminListConsumerGroupsInput, AdminListConsumerGroupsMachine, AdminListConsumerGroupsTerminal,
};

#[test]
fn discovery_then_exact_brokers_filters_sorts_and_deduplicates_consumer_groups() {
    let mut machine = machine();
    let first = effect(
        &mut machine,
        AdminListConsumerGroupsInput::Start {
            now: Moment::from_tick(1),
        },
    );
    assert!(matches!(
        first,
        AdminListConsumerGroupsEffect::SubmitDiscovery { .. }
    ));
    machine
        .apply(AdminListConsumerGroupsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept discovery: {error}"));
    let first_broker = effect(
        &mut machine,
        AdminListConsumerGroupsInput::BrokersDiscovered {
            broker_ids: vec![8, 3],
        },
    );
    assert!(matches!(
        first_broker,
        AdminListConsumerGroupsEffect::SubmitBroker { broker_id: 3, .. }
    ));
    machine
        .apply(AdminListConsumerGroupsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept broker 3: {error}"));
    let second_broker = effect(
        &mut machine,
        AdminListConsumerGroupsInput::BrokerResponded {
            throttle_time_ms: 7,
            outcome: AdminListConsumerGroupsBrokerOutcome::Groups {
                broker_id: 3,
                groups: vec![
                    listing("zeta", "consumer", Some("Stable"), Some("classic")),
                    listing("connect", "connect", Some("Stable"), Some("classic")),
                    listing("same", "consumer", Some("Stable"), Some("classic")),
                ],
            },
        },
    );
    assert!(matches!(
        second_broker,
        AdminListConsumerGroupsEffect::SubmitBroker { broker_id: 8, .. }
    ));
    machine
        .apply(AdminListConsumerGroupsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept broker 8: {error}"));
    let terminal = effect(
        &mut machine,
        AdminListConsumerGroupsInput::BrokerResponded {
            throttle_time_ms: 11,
            outcome: AdminListConsumerGroupsBrokerOutcome::Groups {
                broker_id: 8,
                groups: vec![
                    listing("alpha", "", None, None),
                    listing("modern", "", Some("Stable"), Some("consumer")),
                    listing("same", "consumer", Some("Empty"), Some("classic")),
                ],
            },
        },
    );
    let AdminListConsumerGroupsEffect::Complete {
        terminal: AdminListConsumerGroupsTerminal::Listed(batch),
        ..
    } = terminal
    else {
        panic!("expected listed terminal");
    };
    let (throttle, groups, errors) = batch.into_parts();
    assert_eq!(throttle, 11);
    assert!(errors.is_empty());
    assert_eq!(
        groups
            .iter()
            .map(|group| group.group_id())
            .collect::<Vec<_>>(),
        vec!["alpha", "modern", "same", "zeta"]
    );
    assert_eq!(groups[2].group_state(), Some("Empty"));
}

#[test]
fn exact_broker_error_is_data_and_does_not_abort_remaining_brokers() {
    let mut machine = machine();
    let _ = effect(
        &mut machine,
        AdminListConsumerGroupsInput::Start {
            now: Moment::from_tick(1),
        },
    );
    machine
        .apply(AdminListConsumerGroupsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept discovery: {error}"));
    let _ = effect(
        &mut machine,
        AdminListConsumerGroupsInput::BrokersDiscovered {
            broker_ids: vec![2],
        },
    );
    machine
        .apply(AdminListConsumerGroupsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept broker: {error}"));
    let terminal = effect(
        &mut machine,
        AdminListConsumerGroupsInput::BrokerResponded {
            throttle_time_ms: 4,
            outcome: AdminListConsumerGroupsBrokerOutcome::Rejected(
                AdminListConsumerGroupsBrokerError::new(
                    2,
                    NonZeroI16::new(-17).unwrap_or_else(|| panic!("nonzero")),
                ),
            ),
        },
    );
    let AdminListConsumerGroupsEffect::Complete {
        terminal: AdminListConsumerGroupsTerminal::Listed(batch),
        ..
    } = terminal
    else {
        panic!("expected listed terminal");
    };
    let (_, groups, errors) = batch.into_parts();
    assert!(groups.is_empty());
    assert_eq!(errors[0].clone().into_parts(), (2, -17));
}

fn machine() -> AdminListConsumerGroupsMachine {
    AdminListConsumerGroupsMachine::new(OperationId::from_raw(5), Deadline::from_tick(100))
}

fn listing(
    group_id: &str,
    protocol_type: &str,
    state: Option<&str>,
    group_type: Option<&str>,
) -> AdminConsumerGroupListing {
    AdminConsumerGroupListing::new(
        group_id.to_owned(),
        protocol_type.to_owned(),
        state.map(str::to_owned),
        group_type.map(str::to_owned),
    )
}

fn effect(
    machine: &mut AdminListConsumerGroupsMachine,
    input: AdminListConsumerGroupsInput,
) -> AdminListConsumerGroupsEffect {
    machine
        .apply(input)
        .unwrap_or_else(|error| panic!("transition: {error}"))
        .into_effect()
        .unwrap_or_else(|| panic!("effect"))
}
