//! Stable canonical Kafka feature metadata tests.

use std::time::Duration;

use super::{DescribeFeaturesResult, FinalizedFeature, SupportedFeature};

#[test]
fn result_preserves_canonical_ranges_epoch_and_migration_fact() {
    let result = DescribeFeaturesResult::new(
        Duration::from_millis(19),
        vec![
            SupportedFeature::new(String::from("alpha"), 1, 3),
            SupportedFeature::new(String::from("zeta"), 2, 7),
        ],
        true,
        Some(42),
        vec![FinalizedFeature::new(String::from("alpha"), 2, 2)],
        false,
    );

    assert_eq!(result.throttle_time(), Duration::from_millis(19));
    assert_eq!(result.supported_features()[0].name(), "alpha");
    assert_eq!(result.supported_features()[1].name(), "zeta");
    assert!(result.supported_features_complete());
    assert_eq!(result.finalized_features()[0].name(), "alpha");
    assert_eq!(result.finalized_features_epoch(), Some(42));
    assert!(!result.zk_migration_ready());
}

#[test]
fn absent_epoch_remains_distinct_from_exact_migration_readiness() {
    let result =
        DescribeFeaturesResult::new(Duration::ZERO, Vec::new(), false, None, Vec::new(), true);

    assert!(!result.supported_features_complete());
    assert_eq!(result.finalized_features_epoch(), None);
    assert!(result.zk_migration_ready());
    assert_eq!(
        result.into_parts(),
        (Duration::ZERO, Vec::new(), false, None, Vec::new(), true)
    );
}
