//! Stable generated-free result and exact broker-error translation scenarios.

use core::num::NonZeroI16;

use kafka_client_core::{
    Deadline, ListClientMetricsResourcesBrokerError as CoreBrokerError,
    ListClientMetricsResourcesEffect as CoreEffect, ListClientMetricsResourcesInput as CoreInput,
    ListClientMetricsResourcesMachine as CoreMachine, OperationId,
};

use super::{ListClientMetricsResourcesOutcome, outcome::translate_terminal};

#[test]
fn successful_terminal_preserves_throttle_and_canonical_names() {
    let terminal = complete(CoreInput::BrokerResponded {
        throttle_time_ms: 27,
        resource_names: vec!["zeta".to_owned(), "alpha".to_owned()],
    });
    let ListClientMetricsResourcesOutcome::Listed(listing) = translate_terminal(terminal) else {
        panic!("listing expected");
    };

    assert_eq!(listing.throttle_time_ms(), 27);
    assert_eq!(listing.resource_names(), ["alpha", "zeta"]);
    assert_eq!(
        listing.into_parts(),
        (27, vec!["alpha".to_owned(), "zeta".to_owned()])
    );
}

#[test]
fn exact_signed_top_level_error_and_throttle_remain_lossless() {
    let error = CoreBrokerError::new(
        13,
        NonZeroI16::new(-32_000).unwrap_or_else(|| panic!("nonzero")),
    );
    let terminal = complete(CoreInput::BrokerRejected { error });
    let ListClientMetricsResourcesOutcome::BrokerRejected(error) = translate_terminal(terminal)
    else {
        panic!("broker rejection expected");
    };

    assert_eq!(error.into_parts(), (13, -32_000));
}

fn complete(input: CoreInput) -> kafka_client_core::ListClientMetricsResourcesTerminal {
    let mut machine = CoreMachine::new(OperationId::from_raw(1), Deadline::from_tick(100));
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
