//! Stable generated-free listing and exact broker-error translation.

use core::num::NonZeroI16;

use kafka_client_core::{
    ConfigResourceType, Deadline, ListConfigResourcesBrokerError as CoreBrokerError,
    ListConfigResourcesEffect as CoreEffect, ListConfigResourcesInput as CoreInput,
    ListConfigResourcesMachine as CoreMachine, ListConfigResourcesPlan, ListedConfigResource,
    OperationId,
};

use super::{ListConfigResourcesOutcome, outcome::translate_terminal};

#[test]
fn successful_terminal_preserves_throttle_canonical_resources_and_future_type() {
    let terminal = complete(CoreInput::BrokerResponded {
        throttle_time_ms: 27,
        resources: vec![
            ListedConfigResource::new(future_type(), "zeta".to_owned()),
            ListedConfigResource::new(ConfigResourceType::TOPIC, "orders".to_owned()),
        ],
    });
    let ListConfigResourcesOutcome::Listed(listing) = translate_terminal(terminal) else {
        panic!("listing expected");
    };

    assert_eq!(listing.throttle_time_ms(), 27);
    assert_eq!(listing.resources()[0].resource_type(), 2);
    assert_eq!(listing.resources()[0].name(), "orders");
    assert_eq!(listing.resources()[1].resource_type(), 64);
    assert_eq!(listing.resources()[1].name(), "zeta");
    let (throttle, resources) = listing.into_parts();
    assert_eq!(throttle, 27);
    assert_eq!(resources[1].clone().into_parts(), (64, "zeta".to_owned()));
}

#[test]
fn exact_signed_top_level_error_and_throttle_remain_lossless() {
    let error = CoreBrokerError::new(
        13,
        NonZeroI16::new(-32_000).unwrap_or_else(|| panic!("nonzero")),
    );
    let terminal = complete(CoreInput::BrokerRejected { error });
    let ListConfigResourcesOutcome::BrokerRejected(error) = translate_terminal(terminal) else {
        panic!("broker rejection expected");
    };

    assert_eq!(error.into_parts(), (13, -32_000));
}

fn complete(input: CoreInput) -> kafka_client_core::ListConfigResourcesTerminal {
    let plan = ListConfigResourcesPlan::new(vec![ConfigResourceType::TOPIC, future_type()])
        .unwrap_or_else(|error| panic!("plan: {error}"));
    let mut machine = CoreMachine::new(OperationId::from_raw(1), Deadline::from_tick(100), plan);
    machine
        .apply(CoreInput::Start {
            now: kafka_client_core::Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start: {error}"));
    machine
        .apply(CoreInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept: {error}"));
    let effect = machine
        .apply(input)
        .unwrap_or_else(|error| panic!("complete: {error}"))
        .into_effect();
    let Some(CoreEffect::Complete { terminal, .. }) = effect else {
        panic!("terminal expected");
    };
    terminal
}

fn future_type() -> ConfigResourceType {
    ConfigResourceType::new(64).unwrap_or_else(|error| panic!("future type: {error}"))
}
