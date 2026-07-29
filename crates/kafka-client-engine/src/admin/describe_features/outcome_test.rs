//! Stable generated-free result and exact broker-error translation scenarios.

use core::num::NonZeroI16;

use kafka_client_core::{
    Deadline, DescribeFeaturesBrokerError as CoreBrokerError,
    DescribeFeaturesDescription as CoreDescription, DescribeFeaturesEffect as CoreEffect,
    DescribeFeaturesFinalizedFeature as CoreFinalizedFeature, DescribeFeaturesInput as CoreInput,
    DescribeFeaturesMachine as CoreMachine,
    DescribeFeaturesSupportedFeature as CoreSupportedFeature, OperationId,
};

use super::{DescribeFeaturesOutcome, outcome::translate_terminal};

#[test]
fn successful_terminal_preserves_complete_canonical_feature_metadata() {
    let description = CoreDescription::new(
        27,
        vec![
            CoreSupportedFeature::new("zeta".to_owned(), 1, 4),
            CoreSupportedFeature::new("alpha".to_owned(), 0, 2),
        ],
        false,
        Some(9),
        vec![CoreFinalizedFeature::new("alpha".to_owned(), 1, 2)],
        true,
    )
    .unwrap_or_else(|error| panic!("description: {error}"));
    let terminal = complete(CoreInput::BrokerResponded { description });
    let DescribeFeaturesOutcome::Described(description) = translate_terminal(terminal) else {
        panic!("description expected");
    };

    assert_eq!(description.throttle_time_ms(), 27);
    assert!(!description.supported_features_complete());
    assert_eq!(description.finalized_features_epoch(), Some(9));
    assert!(description.zk_migration_ready());
    assert_eq!(description.supported_features()[0].name(), "alpha");
    assert_eq!(description.supported_features()[1].name(), "zeta");
    assert_eq!(description.finalized_features()[0].name(), "alpha");
}

#[test]
fn exact_signed_top_level_error_and_throttle_remain_lossless() {
    let error = CoreBrokerError::new(
        13,
        NonZeroI16::new(-32_000).unwrap_or_else(|| panic!("nonzero")),
    );
    let terminal = complete(CoreInput::BrokerRejected { error });
    let DescribeFeaturesOutcome::BrokerRejected(error) = translate_terminal(terminal) else {
        panic!("broker rejection expected");
    };

    assert_eq!(error.into_parts(), (13, -32_000));
}

fn complete(input: CoreInput) -> kafka_client_core::DescribeFeaturesTerminal {
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
