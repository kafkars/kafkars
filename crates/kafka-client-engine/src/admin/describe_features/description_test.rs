//! Stable feature-description accessor and ownership scenarios.

use super::{
    DescribeFeaturesDescription, DescribeFeaturesFinalizedFeature, DescribeFeaturesSupportedFeature,
};

#[test]
fn stable_description_preserves_completeness_and_exact_feature_levels() {
    let description = DescribeFeaturesDescription {
        throttle_time_ms: 17,
        supported_features: vec![DescribeFeaturesSupportedFeature {
            name: "metadata.version".to_owned(),
            min_version: 1,
            max_version: 21,
        }],
        supported_features_complete: false,
        finalized_features_epoch: Some(9),
        finalized_features: vec![DescribeFeaturesFinalizedFeature {
            name: "metadata.version".to_owned(),
            min_version_level: 7,
            max_version_level: 7,
        }],
        zk_migration_ready: true,
    };

    assert_eq!(description.throttle_time_ms(), 17);
    assert!(!description.supported_features_complete());
    assert_eq!(
        description.supported_features()[0].name(),
        "metadata.version"
    );
    assert_eq!(description.supported_features()[0].min_version(), 1);
    assert_eq!(description.supported_features()[0].max_version(), 21);
    assert_eq!(description.finalized_features_epoch(), Some(9));
    assert_eq!(description.finalized_features()[0].min_version_level(), 7);
    assert_eq!(description.finalized_features()[0].max_version_level(), 7);
    assert!(description.zk_migration_ready());
}
