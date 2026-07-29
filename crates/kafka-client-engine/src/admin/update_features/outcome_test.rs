//! Stable translation of mixed old-broker results and whole failures.

use core::num::NonZeroI16;

use kafka_client_core::{
    UpdateFeatureOutcome as CoreOutcome, UpdateFeaturesBatch as CoreBatch,
    UpdateFeaturesBrokerError as CoreBrokerError, UpdateFeaturesTerminal as CoreTerminal,
};

use super::{UpdateFeatureResult, UpdateFeaturesOutcome, outcome::translate_terminal};

#[test]
fn mixed_per_feature_results_survive_engine_translation() {
    let error = CoreBrokerError::new(
        NonZeroI16::new(42).unwrap_or_else(|| panic!("nonzero")),
        Some("rejected".to_owned()),
        false,
    );
    let terminal = CoreTerminal::Updated(CoreBatch::new(
        17,
        vec![
            CoreOutcome::updated("metadata.version".to_owned()),
            CoreOutcome::failed("kraft.version".to_owned(), error),
        ],
    ));
    let UpdateFeaturesOutcome::Updated(batch) = translate_terminal(terminal) else {
        panic!("updated batch expected");
    };
    assert_eq!(batch.throttle_time_ms(), 17);
    assert!(matches!(
        batch.outcomes()[0].result(),
        UpdateFeatureResult::Updated
    ));
    let UpdateFeatureResult::Failed(error) = batch.outcomes()[1].result() else {
        panic!("per-feature broker error expected");
    };
    assert_eq!(error.code(), 42);
    assert_eq!(error.message(), Some("rejected"));
}
