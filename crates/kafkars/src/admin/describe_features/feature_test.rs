//! Stable Kafka feature range tests.

use super::{FinalizedFeature, SupportedFeature};

#[test]
fn supported_feature_exposes_exact_name_and_range() {
    let feature = SupportedFeature::new(String::from("metadata.version"), 7, 12);

    assert_eq!(feature.name(), "metadata.version");
    assert_eq!(feature.min_version_level(), 7);
    assert_eq!(feature.max_version_level(), 12);
    assert_eq!(
        feature.into_parts(),
        (String::from("metadata.version"), 7, 12)
    );
}

#[test]
fn finalized_feature_exposes_exact_name_and_range() {
    let feature = FinalizedFeature::new(String::from("transaction.version"), 3, 4);

    assert_eq!(feature.name(), "transaction.version");
    assert_eq!(feature.min_version_level(), 3);
    assert_eq!(feature.max_version_level(), 4);
    assert_eq!(
        feature.into_parts(),
        (String::from("transaction.version"), 3, 4)
    );
}
