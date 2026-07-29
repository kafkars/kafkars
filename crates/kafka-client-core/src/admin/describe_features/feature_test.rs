//! Supported and finalized feature scalar-value scenarios.

use super::{DescribeFeaturesFinalizedFeature, DescribeFeaturesSupportedFeature};

#[test]
fn supported_feature_preserves_complete_range() {
    let feature = DescribeFeaturesSupportedFeature::new("metadata.version".to_owned(), 0, 23);
    assert_eq!(feature.name(), "metadata.version");
    assert_eq!(feature.min_version(), 0);
    assert_eq!(feature.max_version(), 23);
    assert_eq!(feature.into_parts(), ("metadata.version".to_owned(), 0, 23));
}

#[test]
fn finalized_feature_preserves_complete_range() {
    let feature = DescribeFeaturesFinalizedFeature::new("group.version".to_owned(), 1, 2);
    assert_eq!(feature.name(), "group.version");
    assert_eq!(feature.min_version_level(), 1);
    assert_eq!(feature.max_version_level(), 2);
    assert_eq!(feature.into_parts(), ("group.version".to_owned(), 1, 2));
}
